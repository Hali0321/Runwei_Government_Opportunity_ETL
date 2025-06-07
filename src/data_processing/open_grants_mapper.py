#!/usr/bin/env python3
"""
Competitor Format Mapper - Replicates exact competitor structure
Maps grants.gov CSV data to match competitor's frontend format
"""

import pandas as pd
import json
import re
from datetime import datetime
from typing import Dict, List, Optional
import os

class CompetitorFormatMapper:
    def __init__(self):
        self.mapping_rules = self._load_mapping_rules()
        
    def process_grants_data(self, csv_file_path: str) -> List[Dict]:
        """Process grants.gov CSV into competitor format"""
        
        print("🔄 Processing grants.gov data into competitor format...")
        
        # Read the grants.gov CSV
        df = pd.read_csv(csv_file_path)
        print(f"📊 Loaded {len(df)} grant opportunities")
        
        competitor_grants = []
        
        for index, row in df.iterrows():
            try:
                grant_data = self._map_to_competitor_format(row)
                competitor_grants.append(grant_data)
                
                if (index + 1) % 100 == 0:
                    print(f"✅ Processed {index + 1} grants...")
                    
            except Exception as e:
                print(f"⚠️ Error processing row {index}: {e}")
                continue
        
        print(f"🎉 Successfully processed {len(competitor_grants)} grants")
        return competitor_grants
    
    def _map_to_competitor_format(self, row: pd.Series) -> Dict:
        """Map single row to competitor format matching their exact structure"""
        
        return {
            # Title exactly as competitor shows
            "title": self._clean_title(row.get('OPPORTUNITY TITLE', '')),
            
            # Funding amount in competitor's $XX,XXX,XXX format
            "funding_amount": self._format_funding_amount(row),
            
            # Funder mapping to match competitor's agency display
            "funder": self._map_funder(row.get('AGENCY NAME', '')),
            
            # Geographic eligibility - competitor shows "Federal" for most
            "eligible_region": self._determine_eligible_region(row),
            
            # Activity categorization like competitor
            "eligible_activities": self._categorize_activities(row),
            
            # Applicant eligibility simplified like competitor
            "eligible_applicants": self._standardize_applicants(row.get('ELIGIBLE APPLICANTS', '')),
            
            # Grant overview cleaned and formatted
            "grant_overview": self._create_grant_overview(row),
            
            # Additional metadata for functionality
            "opportunity_id": row.get('OPPORTUNITY NUMBER', ''),
            "agency_code": row.get('AGENCY CODE', ''),
            "deadline": self._format_deadline(row.get('CLOSE DATE', '')),
            "posted_date": self._format_date(row.get('POSTED DATE', '')),
            "status": self._determine_status(row.get('OPPORTUNITY STATUS', '')),
            "direct_link": self._extract_direct_link(row),
            "grant_type": row.get('FUNDING INSTRUMENT TYPE', ''),
            "cfda_number": row.get('ASSISTANCE LISTINGS', ''),
            
            # Competitive analysis fields
            "competitiveness_indicators": self._analyze_competitiveness(row),
            "application_difficulty": self._assess_difficulty(row)
        }
    
    def _clean_title(self, title: str) -> str:
        """Clean title to match competitor's format"""
        if not title or pd.isna(title):
            return "Grant Opportunity"
        
        # Remove Excel hyperlink formatting
        clean_title = re.sub(r'=HYPERLINK\(".*?","(.*?)"\)', r'\1', str(title))
        clean_title = re.sub(r'["""]', '"', clean_title)
        clean_title = clean_title.strip()
        
        return clean_title if clean_title else "Grant Opportunity"
    
    def _format_funding_amount(self, row: pd.Series) -> str:
        """Format funding amount like competitor ($XX,XXX,XXX)"""
        
        # Try award ceiling first, then total funding
        amount_fields = ['AWARD CEILING', 'ESTIMATED TOTAL FUNDING', 'AWARD FLOOR']
        
        for field in amount_fields:
            amount = row.get(field, '')
            if amount and str(amount).lower() not in ['nan', 'none', '']:
                try:
                    # Clean and convert amount
                    clean_amount = re.sub(r'[^\d.]', '', str(amount))
                    if clean_amount:
                        formatted_amount = f"${int(float(clean_amount)):,}"
                        return formatted_amount
                except:
                    continue
        
        return "Not specified"
    
    def _map_funder(self, agency_name: str) -> str:
        """Map agency names to match competitor's funder format"""
        
        if not agency_name or pd.isna(agency_name):
            return "Federal Agency"
        
        # Competitor's agency mapping
        agency_mapping = {
            'National Institutes of Health': 'U.S. Department of Health and Human Services (HHS)',
            'National Science Foundation': 'National Science Foundation (NSF)',
            'Office of Naval Research': 'U.S. Department of Defense (DOD)',
            'Department of Defense': 'U.S. Department of Defense (DOD)',
            'Administration for Community Living': 'U.S. Department of Health and Human Services (HHS)',
            'Employment and Training Administration': 'U.S. Department of Labor (DOL)',
            'U.S. Mission to Morocco': 'U.S. Department of State (DOS)',
            'U.S. Mission to Kuwait': 'U.S. Department of State (DOS)',
            'U.S. Mission to Algeria': 'U.S. Department of State (DOS)'
        }
        
        agency_str = str(agency_name)
        
        # Check for exact matches
        for key, mapped_name in agency_mapping.items():
            if key.lower() in agency_str.lower():
                return mapped_name
        
        # Default formatting for unknown agencies
        if 'HHS' in agency_str or 'Health' in agency_str:
            return 'U.S. Department of Health and Human Services (HHS)'
        elif 'DOD' in agency_str or 'Defense' in agency_str:
            return 'U.S. Department of Defense (DOD)'
        elif 'DOS' in agency_str or 'State' in agency_str:
            return 'U.S. Department of State (DOS)'
        elif 'DOL' in agency_str or 'Labor' in agency_str:
            return 'U.S. Department of Labor (DOL)'
        
        return f"U.S. {agency_str}" if not agency_str.startswith('U.S.') else agency_str
    
    def _determine_eligible_region(self, row: pd.Series) -> str:
        """Determine eligible region like competitor (mostly 'Federal')"""
        
        # Most grants.gov opportunities are federal
        applicants = str(row.get('ELIGIBLE APPLICANTS', '')).lower()
        
        if any(term in applicants for term in ['state', 'local', 'county', 'city']):
            return "Federal, State, and Local"
        else:
            return "Federal"
    
    def _categorize_activities(self, row: pd.Series) -> str:
        """Categorize activities to match competitor's format"""
        
        category = str(row.get('CATEGORY OF FUNDING ACTIVITY', ''))
        funding_type = str(row.get('FUNDING INSTRUMENT TYPE', ''))
        
        # Competitor's activity categories
        if 'Research' in category or 'Science' in category:
            return "Research and Development"
        elif 'Education' in category or 'Training' in category:
            return "Education and Training"
        elif 'Health' in category:
            return "Health Services"
        elif 'Infrastructure' in category or 'Construction' in category:
            return "Infrastructure Development"
        elif 'Procurement' in funding_type or 'Contract' in funding_type:
            return "Purchase Materials"
        else:
            return "Other Activities"
    
    def _standardize_applicants(self, applicants: str) -> str:
        """Standardize applicant types like competitor"""
        
        if not applicants or pd.isna(applicants):
            return "See Additional Information"
        
        applicant_str = str(applicants).lower()
        
        # Competitor's standardized categories
        if 'unrestricted' in applicant_str:
            return "Unrestricted"
        elif any(term in applicant_str for term in ['501(c)(3)', 'nonprofit', 'non-profit']):
            return "Nonprofit Organizations"
        elif 'higher education' in applicant_str or 'university' in applicant_str:
            return "Higher Education Institutions"
        elif any(term in applicant_str for term in ['state government', 'local government', 'county']):
            return "State and Local Governments"
        elif 'small business' in applicant_str:
            return "Small Businesses"
        elif 'tribal' in applicant_str or 'native american' in applicant_str:
            return "Tribal Organizations"
        else:
            return "See Additional Information"
    
    def _create_grant_overview(self, row: pd.Series) -> str:
        """Create grant overview matching competitor's format"""
        
        description = row.get('FUNDING DESCRIPTION', '')
        
        if not description or pd.isna(description):
            return "Grant opportunity details to be announced."
        
        # Clean HTML and format like competitor
        clean_desc = re.sub(r'<[^>]+>', '', str(description))
        clean_desc = re.sub(r'\s+', ' ', clean_desc).strip()
        
        # Truncate to competitor's typical length (200-300 words)
        if len(clean_desc) > 800:
            clean_desc = clean_desc[:800] + "..."
        
        return clean_desc
    
    def _extract_direct_link(self, row: pd.Series) -> str:
        """Extract direct application link"""
        
        # Check for direct link in additional information
        link = row.get('LINK TO ADDITIONAL INFORMATION', '')
        if link and 'http' in str(link):
            return str(link)
        
        # Extract from opportunity number if it contains grants.gov link
        opp_number = row.get('OPPORTUNITY NUMBER', '')
        if opp_number and 'grants.gov' in str(opp_number):
            # Extract URL from hyperlink
            match = re.search(r'https://[^"]+', str(opp_number))
            if match:
                return match.group(0)
        
        return "https://www.grants.gov"
    
    def _format_deadline(self, date_str: str) -> str:
        """Format deadline like competitor"""
        if not date_str or pd.isna(date_str):
            return ""
        
        try:
            # Parse date and format consistently
            date_obj = pd.to_datetime(str(date_str))
            return date_obj.strftime("%B %d, %Y")
        except:
            return str(date_str)
    
    def _format_date(self, date_str: str) -> str:
        """Format posted date"""
        return self._format_deadline(date_str)
    
    def _determine_status(self, status: str) -> str:
        """Map status to competitor's format"""
        if not status:
            return "Unknown"
        
        status_map = {
            'Posted': 'Open',
            'Forecasted': 'Forecasted', 
            'Closed': 'Closed',
            'Archived': 'Archived'
        }
        
        return status_map.get(str(status), str(status))
    
    def _analyze_competitiveness(self, row: pd.Series) -> Dict:
        """Analyze competitiveness indicators"""
        
        awards = self._safe_int(row.get('EXPECTED NUMBER OF AWARDS', 1))
        funding = self._safe_int(row.get('AWARD CEILING', 0))
        
        return {
            "estimated_applicants": awards * 15,  # Rough estimate
            "competition_level": "High" if awards < 10 else "Medium" if awards < 50 else "Low",
            "funding_attractiveness": "High" if funding > 1000000 else "Medium" if funding > 100000 else "Low"
        }
    
    def _assess_difficulty(self, row: pd.Series) -> str:
        """Assess application difficulty"""
        
        agency = str(row.get('AGENCY NAME', '')).upper()
        funding = self._safe_int(row.get('AWARD CEILING', 0))
        
        if 'NIH' in agency or funding > 5000000:
            return "High"
        elif funding > 500000:
            return "Medium"
        else:
            return "Low"
    
    def _safe_int(self, value) -> int:
        """Safely convert to integer"""
        try:
            return int(float(str(value).replace(',', '').replace('$', '')))
        except:
            return 0
    
    def _load_mapping_rules(self) -> Dict:
        """Load mapping rules configuration"""
        return {
            "agency_standardization": True,
            "funding_formatting": True,
            "category_mapping": True
        }
    
    def save_to_formats(self, grants_data: List[Dict], output_dir: str = "output"):
        """Save data in multiple formats"""
        
        os.makedirs(output_dir, exist_ok=True)
        
        # JSON format for API
        json_file = f"{output_dir}/competitor_format_grants.json"
        with open(json_file, 'w') as f:
            json.dump(grants_data, f, indent=2)
        print(f"✅ Saved JSON: {json_file}")
        
        # CSV format for analysis
        df = pd.DataFrame(grants_data)
        csv_file = f"{output_dir}/competitor_format_grants.csv"
        df.to_csv(csv_file, index=False)
        print(f"✅ Saved CSV: {csv_file}")
        
        # Summary statistics
        self._generate_summary(grants_data, output_dir)
    
    def _generate_summary(self, grants_data: List[Dict], output_dir: str):
        """Generate summary statistics"""
        
        total_grants = len(grants_data)
        
        # Funding analysis
        funding_amounts = []
        for grant in grants_data:
            amount_str = grant.get('funding_amount', '0')
            if amount_str != "Not specified":
                try:
                    amount = int(amount_str.replace('$', '').replace(',', ''))
                    funding_amounts.append(amount)
                except:
                    pass
        
        # Agency distribution
        agencies = {}
        for grant in grants_data:
            agency = grant.get('funder', 'Unknown')
            agencies[agency] = agencies.get(agency, 0) + 1
        
        summary = {
            "total_grants": total_grants,
            "funding_statistics": {
                "total_opportunities_with_funding": len(funding_amounts),
                "average_funding": sum(funding_amounts) / len(funding_amounts) if funding_amounts else 0,
                "max_funding": max(funding_amounts) if funding_amounts else 0,
                "min_funding": min(funding_amounts) if funding_amounts else 0
            },
            "top_agencies": dict(sorted(agencies.items(), key=lambda x: x[1], reverse=True)[:10]),
            "generated_at": datetime.now().isoformat()
        }
        
        summary_file = f"{output_dir}/competitor_format_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"✅ Saved Summary: {summary_file}")

def main():
    mapper = CompetitorFormatMapper()
    
    # Process the grants.gov data
    csv_file = "src/scripts/grants-gov-opp-search--20250530141151.csv"
    grants_data = mapper.process_grants_data(csv_file)
    
    # Save in multiple formats
    mapper.save_to_formats(grants_data)
    
    print(f"\n🎉 Competitor Format Processing Complete!")
    print(f"📊 Processed {len(grants_data)} grants in competitor format")

if __name__ == "__main__":
    main()
# This code is a standalone script that processes grants.gov data into a competitor's format.
# It reads a CSV file, maps the data to match the competitor's structure, and saves the results in JSON and CSV formats.
# It also generates summary statistics for funding amounts and agency distributions.
# The script is designed to be run directly and will output the processed data to the specified output directory.
# The main function orchestrates the processing and saving of data.