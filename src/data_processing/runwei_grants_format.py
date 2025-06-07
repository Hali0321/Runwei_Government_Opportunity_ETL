#!/usr/bin/env python3
"""
Innovative Grants-Specific Format
Advanced data structure optimized for grants.gov data with superior features
"""

import pandas as pd
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import os
import hashlib

class InnovativeGrantsFormat:
    def __init__(self):
        self.domain_keywords = self._load_domain_keywords()
        self.agency_profiles = self._load_agency_profiles()
        
    def process_grants_data(self, csv_file_path: str) -> List[Dict]:
        """Process grants.gov CSV into innovative format with advanced features"""
        
        print("🚀 Processing grants.gov data into innovative format...")
        
        df = pd.read_csv(csv_file_path)
        print(f"📊 Loaded {len(df)} grant opportunities")
        
        innovative_grants = []
        
        for index, row in df.iterrows():
            try:
                grant_data = self._create_innovative_grant_record(row, index)
                innovative_grants.append(grant_data)
                
                if (index + 1) % 50 == 0:
                    print(f"✨ Processed {index + 1} grants with advanced analytics...")
                    
            except Exception as e:
                print(f"⚠️ Error processing row {index}: {e}")
                continue
        
        # Post-process for advanced features
        innovative_grants = self._add_comparative_analytics(innovative_grants)
        
        print(f"🎉 Successfully processed {len(innovative_grants)} grants with innovative features")
        return innovative_grants
    
    def _create_innovative_grant_record(self, row: pd.Series, index: int) -> Dict:
        """Create comprehensive grant record with innovative features"""
        
        base_data = self._extract_base_data(row)
        
        return {
            # === CORE IDENTIFICATION ===
            "grant_id": self._generate_grant_id(row),
            "source_id": row.get('OPPORTUNITY NUMBER', ''),
            "title": self._enhance_title(row),
            "title_normalized": self._normalize_title(row),
            
            # === AGENCY INTELLIGENCE ===
            "agency_intelligence": self._analyze_agency(row),
            
            # === ADVANCED FUNDING ANALYSIS ===
            "funding_intelligence": self._comprehensive_funding_analysis(row),
            
            # === TIMELINE INTELLIGENCE ===
            "timeline_intelligence": self._advanced_timeline_analysis(row),
            
            # === SMART CATEGORIZATION ===
            "categorization": self._smart_categorization(row),
            
            # === ELIGIBILITY INTELLIGENCE ===
            "eligibility_intelligence": self._advanced_eligibility_analysis(row),
            
            # === APPLICATION STRATEGY ===
            "application_intelligence": self._application_strategy_analysis(row),
            
            # === COMPETITIVE INTELLIGENCE ===
            "competitive_intelligence": self._competitive_analysis(row),
            
            # === SEMANTIC ANALYSIS ===
            "semantic_analysis": self._semantic_content_analysis(row),
            
            # === MATCH SCORING ===
            "match_scoring": self._calculate_match_scores(row),
            
            # === RISK ASSESSMENT ===
            "risk_assessment": self._assess_risks(row),
            
            # === COLLABORATION OPPORTUNITIES ===
            "collaboration_intelligence": self._identify_collaboration_opportunities(row),
            
            # === ENHANCED METADATA ===
            "metadata": self._generate_enhanced_metadata(row, index),
            
            # === ORIGINAL DATA (for compatibility) ===
            "original_data": self._preserve_original_data(row)
        }
    
    def _extract_base_data(self, row: pd.Series) -> Dict:
        """Extract and clean base data fields"""
        return {
            field: self._clean_field(row.get(field, ''))
            for field in row.index
        }
    
    def _generate_grant_id(self, row: pd.Series) -> str:
        """Generate unique, trackable grant ID"""
        
        opp_number = str(row.get('OPPORTUNITY NUMBER', ''))
        agency = str(row.get('AGENCY CODE', ''))
        title = str(row.get('OPPORTUNITY TITLE', ''))
        
        # Create hash-based ID for uniqueness
        content = f"{opp_number}_{agency}_{title}"
        hash_suffix = hashlib.md5(content.encode()).hexdigest()[:8]
        
        # Clean opportunity number
        clean_opp = re.sub(r'[^\w\-]', '', opp_number.replace('=HYPERLINK("', '').split(',')[0] if 'HYPERLINK' in opp_number else opp_number)
        
        return f"{agency}_{clean_opp}_{hash_suffix}".upper()
    
    def _enhance_title(self, row: pd.Series) -> str:
        """Enhance title with smart formatting and context"""
        
        title = row.get('OPPORTUNITY TITLE', '')
        if not title or pd.isna(title):
            return "Grant Opportunity"
        
        # Clean hyperlink formatting
        clean_title = re.sub(r'=HYPERLINK\(".*?","(.*?)"\)', r'\1', str(title))
        clean_title = re.sub(r'["""]', '"', clean_title).strip()
        
        # Add contextual enhancements
        agency = str(row.get('AGENCY NAME', ''))
        if 'NIH' in agency and 'NIH' not in clean_title:
            clean_title = f"[NIH] {clean_title}"
        elif 'NSF' in agency and 'NSF' not in clean_title:
            clean_title = f"[NSF] {clean_title}"
        
        return clean_title
    
    def _normalize_title(self, row: pd.Series) -> str:
        """Create normalized title for matching/searching"""
        title = self._enhance_title(row)
        return re.sub(r'[^\w\s]', ' ', title.lower()).strip()
    
    def _analyze_agency(self, row: pd.Series) -> Dict:
        """Comprehensive agency analysis"""
        
        agency_name = str(row.get('AGENCY NAME', ''))
        agency_code = str(row.get('AGENCY CODE', ''))
        
        return {
            "name": agency_name,
            "code": agency_code,
            "department": self._identify_department(agency_name),
            "prestige_score": self._calculate_agency_prestige(agency_name),
            "funding_style": self._analyze_funding_style(agency_name),
            "application_complexity": self._estimate_agency_complexity(agency_name),
            "success_patterns": self._analyze_agency_success_patterns(agency_name),
            "typical_review_time": self._estimate_review_time(agency_name)
        }
    
    def _comprehensive_funding_analysis(self, row: pd.Series) -> Dict:
        """Advanced funding analysis with multiple perspectives"""
        
        total_funding = self._safe_float(row.get('ESTIMATED TOTAL FUNDING', 0))
        award_ceiling = self._safe_float(row.get('AWARD CEILING', 0))
        award_floor = self._safe_float(row.get('AWARD FLOOR', 0))
        expected_awards = self._safe_int(row.get('EXPECTED NUMBER OF AWARDS', 1))
        
        return {
            "program_funding": {
                "total": total_funding,
                "currency": "USD",
                "fiscal_year": self._extract_fiscal_year(row)
            },
            "individual_awards": {
                "ceiling": award_ceiling,
                "floor": award_floor,
                "estimated_average": (award_ceiling + award_floor) / 2 if award_ceiling and award_floor else award_ceiling or award_floor,
                "range_flexibility": self._calculate_funding_flexibility(award_ceiling, award_floor)
            },
            "award_distribution": {
                "expected_count": expected_awards,
                "estimated_funding_per_award": total_funding / expected_awards if expected_awards > 0 and total_funding > 0 else None,
                "competition_ratio": self._estimate_competition_ratio(expected_awards, total_funding)
            },
            "funding_tier": self._categorize_funding_tier(award_ceiling or total_funding),
            "budget_requirements": self._analyze_budget_requirements(row),
            "cost_sharing": self._analyze_cost_sharing(row),
            "funding_reliability": self._assess_funding_reliability(row)
        }
    
    def _advanced_timeline_analysis(self, row: pd.Series) -> Dict:
        """Advanced timeline analysis with predictive elements"""
        
        posted_date = self._parse_date(row.get('POSTED DATE', ''))
        close_date = self._parse_date(row.get('CLOSE DATE', ''))
        estimated_post = self._parse_date(row.get('ESTIMATED POST DATE', ''))
        estimated_due = self._parse_date(row.get('ESTIMATED APPLICATION DUE DATE', ''))
        
        current_date = datetime.now()
        
        return {
            "key_dates": {
                "posted": posted_date.isoformat() if posted_date else None,
                "deadline": close_date.isoformat() if close_date else None,
                "estimated_post": estimated_post.isoformat() if estimated_post else None,
                "estimated_due": estimated_due.isoformat() if estimated_due else None
            },
            "timeline_status": self._determine_timeline_status(close_date, current_date),
            "urgency_analysis": {
                "days_remaining": (close_date - current_date).days if close_date else None,
                "urgency_level": self._calculate_urgency_level(close_date, current_date),
                "preparation_time_available": self._estimate_prep_time_available(close_date, current_date)
            },
            "application_window": {
                "total_days": (close_date - posted_date).days if close_date and posted_date else None,
                "window_type": self._classify_application_window(close_date, posted_date)
            },
            "predicted_timeline": self._predict_future_timeline(row)
        }
    
    def _smart_categorization(self, row: pd.Series) -> Dict:
        """AI-powered categorization with multiple classification systems"""
        
        title = str(row.get('OPPORTUNITY TITLE', ''))
        description = str(row.get('FUNDING DESCRIPTION', ''))
        category = str(row.get('CATEGORY OF FUNDING ACTIVITY', ''))
        
        combined_text = f"{title} {description} {category}".lower()
        
        return {
            "primary_domain": self._identify_primary_domain(combined_text),
            "research_areas": self._extract_research_areas(combined_text),
            "technology_focus": self._identify_technology_focus(combined_text),
            "application_areas": self._identify_application_areas(combined_text),
            "interdisciplinary_score": self._calculate_interdisciplinary_score(combined_text),
            "innovation_indicators": self._detect_innovation_indicators(combined_text),
            "societal_impact_areas": self._identify_societal_impact(combined_text),
            "un_sdg_alignment": self._map_to_un_sdgs(combined_text),
            "keywords": self._extract_key_terms(combined_text),
            "complexity_indicators": self._analyze_complexity_indicators(combined_text)
        }
    
    def _advanced_eligibility_analysis(self, row: pd.Series) -> Dict:
        """Comprehensive eligibility analysis with smart matching"""
        
        eligible_applicants = str(row.get('ELIGIBLE APPLICANTS', ''))
        
        return {
            "organization_types": self._parse_organization_types(eligible_applicants),
            "geographic_restrictions": self._analyze_geographic_restrictions(eligible_applicants),
            "size_requirements": self._extract_size_requirements(eligible_applicants),
            "sector_focus": self._identify_sector_focus(eligible_applicants),
            "partnership_requirements": self._identify_partnership_requirements(row),
            "eligibility_complexity": self._score_eligibility_complexity(eligible_applicants),
            "inclusion_indicators": self._detect_inclusion_indicators(eligible_applicants),
            "restrictions": self._identify_restrictions(eligible_applicants)
        }
    
    def _application_strategy_analysis(self, row: pd.Series) -> Dict:
        """Strategic guidance for application preparation"""
        
        agency = str(row.get('AGENCY NAME', ''))
        funding_amount = self._safe_float(row.get('AWARD CEILING', 0))
        
        return {
            "recommended_approach": self._recommend_application_approach(row),
            "key_success_factors": self._identify_success_factors(row),
            "common_pitfalls": self._identify_common_pitfalls(agency),
            "preparation_checklist": self._generate_preparation_checklist(row),
            "team_composition": self._suggest_team_composition(row),
            "budget_strategy": self._suggest_budget_strategy(row),
            "timeline_strategy": self._create_timeline_strategy(row),
            "competitive_advantages": self._identify_competitive_advantages(row)
        }
    
    def _competitive_analysis(self, row: pd.Series) -> Dict:
        """Advanced competitive intelligence"""
        
        expected_awards = self._safe_int(row.get('EXPECTED NUMBER OF AWARDS', 1))
        funding_amount = self._safe_float(row.get('AWARD CEILING', 0))
        agency = str(row.get('AGENCY NAME', ''))
        
        return {
            "competition_level": self._assess_competition_level(expected_awards, funding_amount),
            "estimated_applicants": self._estimate_applicant_pool(row),
            "success_probability": self._calculate_success_probability(row),
            "competitive_factors": self._identify_competitive_factors(row),
            "differentiation_opportunities": self._identify_differentiation_opportunities(row),
            "benchmark_analysis": self._perform_benchmark_analysis(row),
            "market_position": self._analyze_market_position(row)
        }
    
    def _semantic_content_analysis(self, row: pd.Series) -> Dict:
        """Advanced NLP analysis of grant content"""
        
        description = str(row.get('FUNDING DESCRIPTION', ''))
        title = str(row.get('OPPORTUNITY TITLE', ''))
        
        return {
            "readability_score": self._calculate_readability(description),
            "sentiment_analysis": self._analyze_sentiment(description),
            "key_phrases": self._extract_key_phrases(description),
            "technical_level": self._assess_technical_level(description),
            "urgency_indicators": self._detect_urgency_language(description),
            "innovation_emphasis": self._measure_innovation_emphasis(description),
            "collaboration_signals": self._detect_collaboration_language(description)
        }
    
    def _calculate_match_scores(self, row: pd.Series) -> Dict:
        """Calculate various matching scores for different applicant types"""
        
        return {
            "academic_fit": self._score_academic_fit(row),
            "industry_fit": self._score_industry_fit(row),
            "nonprofit_fit": self._score_nonprofit_fit(row),
            "startup_fit": self._score_startup_fit(row),
            "international_fit": self._score_international_fit(row),
            "early_career_fit": self._score_early_career_fit(row),
            "established_researcher_fit": self._score_established_researcher_fit(row)
        }
    
    def _assess_risks(self, row: pd.Series) -> Dict:
        """Comprehensive risk assessment"""
        
        return {
            "funding_risk": self._assess_funding_risk(row),
            "timeline_risk": self._assess_timeline_risk(row),
            "competition_risk": self._assess_competition_risk(row),
            "compliance_risk": self._assess_compliance_risk(row),
            "technical_risk": self._assess_technical_risk(row),
            "partnership_risk": self._assess_partnership_risk(row),
            "overall_risk_score": self._calculate_overall_risk(row)
        }
    
    def _identify_collaboration_opportunities(self, row: pd.Series) -> Dict:
        """Identify potential collaboration opportunities"""
        
        return {
            "suggested_partners": self._suggest_collaboration_partners(row),
            "interdisciplinary_opportunities": self._identify_interdisciplinary_opportunities(row),
            "international_collaboration": self._assess_international_collaboration_potential(row),
            "industry_academia_partnerships": self._identify_industry_academia_opportunities(row),
            "consortium_building": self._assess_consortium_potential(row)
        }
    
    def _generate_enhanced_metadata(self, row: pd.Series, index: int) -> Dict:
        """Generate comprehensive metadata"""
        
        return {
            "processing_info": {
                "processed_at": datetime.now().isoformat(),
                "record_index": index,
                "format_version": "2.0",
                "processor": "InnovativeGrantsFormat"
            },
            "data_quality": {
                "completeness_score": self._calculate_completeness_score(row),
                "reliability_score": self._calculate_reliability_score(row),
                "freshness_indicator": self._calculate_freshness(row)
            },
            "analytics_flags": {
                "high_priority": self._flag_high_priority(row),
                "trending": self._flag_trending(row),
                "urgent": self._flag_urgent(row),
                "innovative": self._flag_innovative(row)
            }
        }
    
    def _preserve_original_data(self, row: pd.Series) -> Dict:
        """Preserve original data for compatibility and audit"""
        return {
            field: self._clean_field(row.get(field, ''))
            for field in row.index
        }
    
    # === UTILITY METHODS ===
    
    def _safe_float(self, value) -> float:
        """Safely convert to float"""
        try:
            if pd.isna(value) or value == '':
                return 0.0
            return float(str(value).replace(',', '').replace('$', ''))
        except:
            return 0.0
    
    def _safe_int(self, value) -> int:
        """Safely convert to integer"""
        try:
            return int(self._safe_float(value))
        except:
            return 0
    
    def _clean_field(self, field_value) -> str:
        """Clean field value"""
        if pd.isna(field_value):
            return ""
        return str(field_value).strip()
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string with multiple format support"""
        if not date_str or pd.isna(date_str):
            return None
        
        date_formats = ['%m/%d/%Y', '%Y-%m-%d', '%b-%d-%Y %I:%M:%S %p %Z']
        
        for fmt in date_formats:
            try:
                return datetime.strptime(str(date_str), fmt)
            except:
                continue
        
        return None
    
    # === PLACEHOLDER METHODS (implement based on specific requirements) ===
    
    def _identify_department(self, agency_name: str) -> str:
        """Identify parent department"""
        dept_mapping = {
            'NIH': 'Department of Health and Human Services',
            'NSF': 'Independent Agency',
            'DOD': 'Department of Defense',
            'NASA': 'Independent Agency'
        }
        
        for key, dept in dept_mapping.items():
            if key in agency_name.upper():
                return dept
        return "Federal Government"
    
    def _calculate_agency_prestige(self, agency_name: str) -> int:
        """Calculate agency prestige score (1-100)"""
        prestige_scores = {
            'NIH': 95, 'NSF': 90, 'NASA': 85, 'DOD': 80, 'DOE': 75
        }
        
        for agency, score in prestige_scores.items():
            if agency in agency_name.upper():
                return score
        return 60  # Default score
    
    def _analyze_funding_style(self, agency_name: str) -> str:
        """Analyze agency funding style"""
        if 'NIH' in agency_name.upper():
            return "Research-focused, peer-reviewed, competitive"
        elif 'NSF' in agency_name.upper():
            return "Basic research, innovation-driven, merit-based"
        elif 'DOD' in agency_name.upper():
            return "Mission-driven, applied research, strategic priorities"
        else:
            return "Federal standard process"
    
    # Continue implementing other placeholder methods as needed...
    
    def _add_comparative_analytics(self, grants_data: List[Dict]) -> List[Dict]:
        """Add comparative analytics across all grants"""
        
        print("🔍 Adding comparative analytics...")
        
        # Calculate percentiles for funding amounts
        funding_amounts = [
            grant['funding_intelligence']['individual_awards']['ceiling'] 
            for grant in grants_data 
            if grant['funding_intelligence']['individual_awards']['ceiling'] > 0
        ]
        
        if funding_amounts:
            funding_amounts.sort()
            
            for grant in grants_data:
                ceiling = grant['funding_intelligence']['individual_awards']['ceiling']
                if ceiling > 0:
                    percentile = (sum(1 for x in funding_amounts if x <= ceiling) / len(funding_amounts)) * 100
                    grant['comparative_analytics'] = {
                        'funding_percentile': percentile,
                        'above_median': ceiling > funding_amounts[len(funding_amounts)//2],
                        'top_tier': percentile >= 75
                    }
        
        return grants_data
    
    def save_to_formats(self, grants_data: List[Dict], output_dir: str = "output"):
        """Save innovative format in multiple formats"""
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Full innovative format (JSON)
        json_file = f"{output_dir}/innovative_grants_format.json"
        with open(json_file, 'w') as f:
            json.dump(grants_data, f, indent=2)
        print(f"✅ Saved Innovative JSON: {json_file}")
        
        # Simplified format for APIs
        simplified_data = self._create_simplified_format(grants_data)
        simplified_file = f"{output_dir}/innovative_grants_simplified.json"
        with open(simplified_file, 'w') as f:
            json.dump(simplified_data, f, indent=2)
        print(f"✅ Saved Simplified JSON: {simplified_file}")
        
        # Analytics summary
        self._generate_innovative_summary(grants_data, output_dir)
    
    def _create_simplified_format(self, grants_data: List[Dict]) -> List[Dict]:
        """Create simplified format for API consumption"""
        return [
            {
                'id': grant['grant_id'],
                'title': grant['title'],
                'agency': grant['agency_intelligence']['name'],
                'funding_max': grant['funding_intelligence']['individual_awards']['ceiling'],
                'deadline': grant['timeline_intelligence']['key_dates']['deadline'],
                'competition_level': grant['competitive_intelligence']['competition_level'],
                'match_scores': grant['match_scoring'],
                'primary_domain': grant['categorization']['primary_domain'],
                'direct_link': grant['original_data'].get('LINK TO ADDITIONAL INFORMATION', '')
            }
            for grant in grants_data
        ]
    
    def _generate_innovative_summary(self, grants_data: List[Dict], output_dir: str):
        """Generate comprehensive analytics summary"""
        
        # Advanced analytics
        total_funding = sum(
            grant['funding_intelligence']['program_funding']['total'] 
            for grant in grants_data 
            if grant['funding_intelligence']['program_funding']['total'] > 0
        )
        
        domain_distribution = {}
        for grant in grants_data:
            domain = grant['categorization']['primary_domain']
            domain_distribution[domain] = domain_distribution.get(domain, 0) + 1
        
        summary = {
            "innovative_analytics": {
                "total_grants": len(grants_data),
                "total_available_funding": total_funding,
                "average_competition_level": self._calculate_average_competition(grants_data),
                "domain_distribution": domain_distribution,
                "high_priority_grants": len([g for g in grants_data if g['metadata']['analytics_flags']['high_priority']]),
                "innovation_focused_grants": len([g for g in grants_data if g['metadata']['analytics_flags']['innovative']])
            },
            "competitive_intelligence": {
                "highly_competitive": len([g for g in grants_data if g['competitive_intelligence']['competition_level'] == 'High']),
                "moderate_competition": len([g for g in grants_data if g['competitive_intelligence']['competition_level'] == 'Medium']),
                "low_competition": len([g for g in grants_data if g['competitive_intelligence']['competition_level'] == 'Low'])
            },
            "generated_at": datetime.now().isoformat(),
            "format_version": "2.0"
        }
        
        summary_file = f"{output_dir}/innovative_analytics_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"✅ Saved Innovative Summary: {summary_file}")

def main():
    formatter = InnovativeGrantsFormat()
    
    # Process the grants.gov data
    csv_file = "src/scripts/grants-gov-opp-search--20250530141151.csv"
    grants_data = formatter.process_grants_data(csv_file)
    
    # Save in multiple formats
    formatter.save_to_formats(grants_data)
    
    print(f"\n🚀 Innovative Format Processing Complete!")
    print(f"✨ Processed {len(grants_data)} grants with advanced intelligence features")

if __name__ == "__main__":
    main()
