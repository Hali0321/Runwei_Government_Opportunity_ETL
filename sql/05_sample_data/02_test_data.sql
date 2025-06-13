-- ===================================
-- GRANTS.GOV API AZURE - TEST DATA
-- ===================================

USE GrantsGovDB;
GO

-- Insert sample grants data with varied conditions
INSERT INTO CleanedGrantsLayer2 (
    OpportunityID, Title, AgencyID, CategoryID, Description, 
    AwardCeiling, AwardFloor, EstimatedTotalFunding, ExpectedAwards,
    CFDANumbers, PostedDate, CloseDate, DaysUntilDeadline,
    Status, EligibilityFullText, Keywords, GrantsGovURL, AdditionalInfoURL,
    ContactEmail, ContactPhone, IsActive, DataQualityScore
)
SELECT
    'GRANT-2023-' + RIGHT('00000' + CAST(ROW_NUMBER() OVER(ORDER BY a.AgencyID) AS VARCHAR), 5),
    CASE 
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 0 THEN a.AgencyName + ' Research Grant'
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 1 THEN a.AgencyName + ' Program Development'
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 2 THEN a.AgencyName + ' Innovation Fund'
        ELSE a.AgencyName + ' Educational Initiative'
    END,
    a.AgencyID,
    c.CategoryID,
    'This grant provides funding for ' + c.CategoryName + ' projects related to ' + 
    CASE 
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 3 = 0 THEN 'research and development'
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 3 = 1 THEN 'education and training'
        ELSE 'community implementation'
    END,
    CASE 
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 0 THEN 1000000
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 1 THEN 500000
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 2 THEN 250000
        ELSE 100000
    END,
    CASE
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 3 = 0 THEN 50000
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 3 = 1 THEN 25000
        ELSE 10000
    END,
    CASE
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 0 THEN 5000000
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 1 THEN 2500000
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 2 THEN 1000000
        ELSE 500000
    END,
    CASE
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 0 THEN 10
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 1 THEN 5
        WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 2 THEN 3
        ELSE 1
    END,
    a.AgencyCode + '.' + CAST(10000 + ROW_NUMBER() OVER(ORDER BY a.AgencyID) AS VARCHAR),
    DATEADD(DAY, -CASE WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 5 = 0 THEN 30
                      WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 5 = 1 THEN 45
                      WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 5 = 2 THEN 60
                      WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 5 = 3 THEN 90
                      ELSE 120 END, GETDATE()),
    DATEADD(DAY, CASE WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 0 THEN 30
                    WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 1 THEN 60
                    WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 2 THEN 90
                    ELSE 120 END, GETDATE()),
    CASE WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 0 THEN 30
         WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 1 THEN 60
         WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 4 = 2 THEN 90
         ELSE 120 END,
    CASE WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 5 = 0 THEN 'Closing This Month'
         WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 5 = 1 THEN 'Open'
         WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 5 = 2 THEN 'Closing Soon'
         WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 5 = 3 THEN 'Archived'
         ELSE 'Posted' END,
    'Eligibility limited to ' + 
    CASE WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 3 = 0 THEN 'public and private institutions'
         WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 3 = 1 THEN 'nonprofit organizations'
         ELSE 'state and local governments' END,
    c.Keywords,
    'https://grants.gov/opportunities/' + 'GRANT-2023-' + RIGHT('00000' + CAST(ROW_NUMBER() OVER(ORDER BY a.AgencyID) AS VARCHAR), 5),
    a.WebsiteURL + '/grants/' + CAST(10000 + ROW_NUMBER() OVER(ORDER BY a.AgencyID) AS VARCHAR),
    'contact' + CAST(ROW_NUMBER() OVER(ORDER BY a.AgencyID) AS VARCHAR) + '@' + LOWER(REPLACE(a.AgencyCode, ' ', '')) + '.gov',
    '(800) 555-' + RIGHT('0000' + CAST(1000 + ROW_NUMBER() OVER(ORDER BY a.AgencyID) AS VARCHAR), 4),
    CASE WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 10 = 0 THEN 0 ELSE 1 END,
    CASE WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 5 = 0 THEN 95
         WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 5 = 1 THEN 90
         WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 5 = 2 THEN 85
         WHEN ROW_NUMBER() OVER(ORDER BY a.AgencyID) % 5 = 3 THEN 80
         ELSE 75 END
FROM AgencyMasterLayer2 a
CROSS JOIN CategoryMasterLayer2 c
WHERE a.IsActive = 1 AND c.IsActive = 1
ORDER BY NEWID();
GO

-- Populate grant eligibility mapping
INSERT INTO GrantEligibilityLayer2 (GrantID, EligibilityID)
SELECT g.GrantID, e.EligibilityID
FROM CleanedGrantsLayer2 g
CROSS JOIN EligibilityMasterLayer2 e
WHERE g.IsActive = 1
AND (g.GrantID % 3) + 1 = (e.EligibilityID % 3) + 1;
GO

-- Populate grant geographic coverage mapping
INSERT INTO GrantGeographicCoverageLayer2 (GrantID, GeographicID)
SELECT g.GrantID, gc.GeographicID
FROM CleanedGrantsLayer2 g
CROSS JOIN GeographicCoverageMasterLayer2 gc
WHERE g.IsActive = 1
AND (g.GrantID % 4) + 1 = (gc.GeographicID % 4) + 1;
GO

-- Populate business layer with analytics
INSERT INTO GrantBusinessViewLayer3 (
    GrantID, ApplicantCount, CompetitionLevel, FundingTier,
    PastCompetitionLevel, RecommendedFocus
)
SELECT 
    g.GrantID,
    CASE WHEN g.ExpectedAwards > 0 THEN g.ExpectedAwards * (2 + (g.GrantID % 5)) ELSE 0 END,
    CASE 
        WHEN g.AwardCeiling > 500000 THEN 'High Competition'
        WHEN g.AwardCeiling > 200000 THEN 'Medium Competition'
        ELSE 'Low Competition'
    END,
    CASE 
        WHEN g.AwardCeiling >= 1000000 THEN 'Tier 1 - Large'
        WHEN g.AwardCeiling >= 500000 THEN 'Tier 2 - Significant'
        WHEN g.AwardCeiling >= 100000 THEN 'Tier 3 - Medium'
        ELSE 'Tier 4 - Small'
    END,
    CASE 
        WHEN g.GrantID % 4 = 0 THEN 'Very High'
        WHEN g.GrantID % 4 = 1 THEN 'High'
        WHEN g.GrantID % 4 = 2 THEN 'Moderate'
        ELSE 'Low'
    END,
    CASE 
        WHEN g.GrantID % 3 = 0 THEN 'Technical Approach'
        WHEN g.GrantID % 3 = 1 THEN 'Budget Justification'
        ELSE 'Partnership Development'
    END
FROM CleanedGrantsLayer2 g
WHERE g.IsActive = 1;
GO

-- Populate SuccessFactors table
INSERT INTO SuccessFactorsLayer3 (
    GrantID, SuccessMetric, SuccessScore, KeyCompetitiveFactors,
    RecommendedApproach, PastSuccessTemplates, RequiredCapabilities
)
SELECT 
    g.GrantID,
    CASE 
        WHEN g.GrantID % 3 = 0 THEN 'Innovation Index'
        WHEN g.GrantID % 3 = 1 THEN 'Implementation Feasibility'
        ELSE 'Cost-Benefit Ratio'
    END,
    55 + (g.GrantID % 40),
    CASE 
        WHEN g.GrantID % 4 = 0 THEN 'Strong preliminary data, experienced team'
        WHEN g.GrantID % 4 = 1 THEN 'Novel approach, clear methodology'
        WHEN g.GrantID % 4 = 2 THEN 'Demonstrated need, community partnerships'
        ELSE 'Cost efficiency, sustainability plan'
    END,
    CASE 
        WHEN g.GrantID % 3 = 0 THEN 'Focus on innovation and novel approaches'
        WHEN g.GrantID % 3 = 1 THEN 'Emphasize implementation timeline and milestones'
        ELSE 'Highlight partnerships and sustainability'
    END,
    CASE 
        WHEN g.GrantID % 3 = 0 THEN 'Research-focused template with strong methodology'
        WHEN g.GrantID % 3 = 1 THEN 'Implementation-focused template with clear timeline'
        ELSE 'Community-focused template with strong partnerships'
    END,
    CASE 
        WHEN g.GrantID % 4 = 0 THEN 'Strong research background, publication history'
        WHEN g.GrantID % 4 = 1 THEN 'Project management expertise, implementation experience'
        WHEN g.GrantID % 4 = 2 THEN 'Community engagement, partnership development'
        ELSE 'Budget management, resource allocation expertise'
    END
FROM CleanedGrantsLayer2 g
WHERE g.IsActive = 1;
GO

-- Populate Agency Stats
INSERT INTO AgencyStatsLayer3 (
    AgencyID, TotalGrants, ActiveGrants, ClosedGrants, TotalFunding,
    AvgFunding, CategoryCount, AvgApplicationPeriod
)
SELECT 
    a.AgencyID,
    COUNT(g.GrantID),
    COUNT(CASE WHEN g.CloseDate >= GETDATE() THEN g.GrantID END),
    COUNT(CASE WHEN g.CloseDate < GETDATE() THEN g.GrantID END),
    SUM(g.EstimatedTotalFunding),
    AVG(g.AwardCeiling),
    COUNT(DISTINCT g.CategoryID),
    AVG(DATEDIFF(DAY, g.PostedDate, g.CloseDate))
FROM AgencyMasterLayer2 a
LEFT JOIN CleanedGrantsLayer2 g ON a.AgencyID = g.AgencyID
WHERE a.IsActive = 1
GROUP BY a.AgencyID;
GO

-- Populate Category Stats
INSERT INTO CategoryStatsLayer3 (
    CategoryID, TotalGrants, ActiveGrants, TotalFunding, AvgFunding,
    AgencyCount, TopAgency, AvgApplicationWindow
)
SELECT 
    c.CategoryID,
    COUNT(g.GrantID),
    COUNT(CASE WHEN g.CloseDate >= GETDATE() THEN g.GrantID END),
    SUM(g.EstimatedTotalFunding),
    AVG(g.AwardCeiling),
    COUNT(DISTINCT g.AgencyID),
    (SELECT TOP 1 a.AgencyName 
     FROM CleanedGrantsLayer2 g2 
     JOIN AgencyMasterLayer2 a ON g2.AgencyID = a.AgencyID 
     WHERE g2.CategoryID = c.CategoryID 
     GROUP BY a.AgencyName 
     ORDER BY COUNT(g2.GrantID) DESC),
    AVG(DATEDIFF(DAY, g.PostedDate, g.CloseDate))
FROM CategoryMasterLayer2 c
LEFT JOIN CleanedGrantsLayer2 g ON c.CategoryID = g.CategoryID
WHERE c.IsActive = 1
GROUP BY c.CategoryID;
GO

-- Populate Trend Analysis
INSERT INTO TrendAnalysisLayer3 (
    AnalysisDate, AnalysisType, TotalOpportunities, NewOpportunities,
    TotalFunding, AvgFunding, LargeFundingCount, MediumFundingCount,
    SmallFundingCount, FundingTrend, OpportunityTrend, TopFundingAgency,
    TopFundingCategory, IsActive
)
VALUES
(DATEADD(MONTH, -3, GETDATE()), 'Quarterly', 250, 75, 125000000, 500000, 50, 100, 100, 0.05, 0.08, 'NIH', 'Health', 1),
(DATEADD(MONTH, -2, GETDATE()), 'Monthly', 85, 25, 42000000, 494000, 18, 32, 35, 0.03, 0.04, 'NSF', 'Science and Technology', 1),
(DATEADD(MONTH, -1, GETDATE()), 'Monthly', 88, 28, 45000000, 511000, 20, 33, 35, 0.07, 0.035, 'NIH', 'Health', 1),
(GETDATE(), 'Current', 92, 30, 48000000, 521000, 22, 35, 35, 0.065, 0.045, 'NIH', 'Health', 1);
GO