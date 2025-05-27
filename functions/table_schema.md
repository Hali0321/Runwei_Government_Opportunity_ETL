# Grant Opportunity Table Schema

## Table Name: GrantOpportunities

### Partition and Row Keys
- PartitionKey: "Grant" (constant for all entries)
- RowKey: {OpportunityID} (unique identifier from Grants.gov)

### Fields
- Title: The title/name of the opportunity
- OpportunityURL: Direct link to the opportunity
- Deadline: Close date of the opportunity
- AwardValue: Estimated funding amount
- ShortDescription: Brief description of the opportunity
- LongDescription: Full description text
- EligibilityInfo: Eligibility requirements
- DatePosted: Date the grant was posted
- Industry: Related industries or categories
- AgencyName: Name of the sponsoring agency
- AgencyCode: Agency identifier
- FundingType: Type of funding instrument
- TargetCommunity: Target demographic information
- ContactEmail: Email for inquiries
- GeographicEligibility: Eligible locations
- OpportunityCategory: Category of opportunity
- CFDA_Numbers: Assistance listing numbers
- LastUpdated: When the record was last updated
