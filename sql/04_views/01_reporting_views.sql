-- ===================================
-- GRANTS.GOV API AZURE - REPORTING VIEWS
-- ===================================

USE GrantsGovDB;
GO

-- Active Grants View
CREATE OR ALTER VIEW vw_ActiveGrants AS
SELECT 
    g.GrantID,
    g.OpportunityID,
    g.Title,
    a.AgencyName,
    c.CategoryName,
    g.AwardCeiling,
    g.AwardFloor,
    g.EstimatedTotalFunding,
    g.ExpectedAwards,
    g.PostedDate,
    g.CloseDate,
    g.DaysUntilDeadline,
    g.Status,
    g.GrantsGovURL,
    g.ContactEmail,
    g.AdditionalInfoURL,
    g.DataQualityScore
FROM CleanedGrantsLayer2 g
LEFT JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
LEFT JOIN CategoryMasterLayer2 c ON g.CategoryID = c.CategoryID
WHERE g.IsActive = 1 
AND g.CloseDate >= GETDATE()
AND g.Status IN ('Open', 'Closing Soon', 'Closing This Month');
GO

-- High Value Grants
CREATE OR ALTER VIEW vw_HighValueGrants AS
SELECT 
    g.GrantID,
    g.OpportunityID,
    g.Title,
    a.AgencyName,
    c.CategoryName,
    g.AwardCeiling,
    g.EstimatedTotalFunding,
    g.ExpectedAwards,
    g.CloseDate,
    g.DaysUntilDeadline,
    b.CompetitionLevel,
    b.FundingTier
FROM CleanedGrantsLayer2 g
LEFT JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
LEFT JOIN CategoryMasterLayer2 c ON g.CategoryID = c.CategoryID
LEFT JOIN GrantBusinessViewLayer3 b ON g.GrantID = b.GrantID
WHERE g.IsActive = 1 
AND g.AwardCeiling >= 500000
AND g.CloseDate >= GETDATE();
GO

-- Grants Summary
CREATE OR ALTER VIEW vw_GrantsSummary AS
SELECT 
    COUNT(*) as TotalGrants,
    COUNT(CASE WHEN CloseDate >= GETDATE() THEN 1 END) as ActiveGrants,
    COUNT(CASE WHEN DaysUntilDeadline <= 7 AND CloseDate >= GETDATE() THEN 1 END) as ClosingSoon,
    COUNT(CASE WHEN AwardCeiling >= 1000000 THEN 1 END) as LargeFundingGrants,
    COUNT(CASE WHEN AwardCeiling BETWEEN 100000 AND 999999 THEN 1 END) as MediumFundingGrants,
    COUNT(CASE WHEN AwardCeiling < 100000 AND AwardCeiling > 0 THEN 1 END) as SmallFundingGrants,
    SUM(AwardCeiling) as TotalFundingAvailable,
    AVG(AwardCeiling) as AvgFundingAmount,
    COUNT(DISTINCT AgencyID) as UniqueAgencies,
    COUNT(DISTINCT CategoryID) as UniqueCategories
FROM CleanedGrantsLayer2
WHERE IsActive = 1;
GO

-- Agency Performance View
CREATE OR ALTER VIEW vw_AgencyPerformance AS
SELECT
    a.AgencyID,
    a.AgencyName,
    a.AgencyCode,
    s.TotalGrants,
    s.ActiveGrants,
    s.ClosedGrants,
    s.TotalFunding,
    s.AvgFunding,
    s.CategoryCount,
    s.AvgApplicationPeriod,
    a.DataQualityScore AS AgencyDataQuality,
    CASE 
        WHEN s.TotalGrants > 100 THEN 'High Volume'
        WHEN s.TotalGrants > 50 THEN 'Medium Volume'
        ELSE 'Low Volume'
    END AS AgencyVolume,
    CASE 
        WHEN s.TotalFunding > 10000000 THEN 'Major Funder'
        WHEN s.TotalFunding > 1000000 THEN 'Significant Funder'
        ELSE 'Minor Funder'
    END AS FunderCategory
FROM AgencyMasterLayer2 a
LEFT JOIN AgencyStatsLayer3 s ON a.AgencyID = s.AgencyID
WHERE a.IsActive = 1;
GO

-- Category Analysis View
CREATE OR ALTER VIEW vw_CategoryAnalysis AS
SELECT
    c.CategoryID,
    c.CategoryName,
    c.CategoryGroup,
    s.TotalGrants,
    s.ActiveGrants,
    s.TotalFunding,
    s.AvgFunding,
    s.AgencyCount,
    s.TopAgency,
    s.AvgApplicationWindow,
    c.Keywords,
    CASE 
        WHEN s.TotalGrants > 50 THEN 'Popular'
        WHEN s.TotalGrants > 20 THEN 'Common'
        ELSE 'Niche'
    END AS CategoryPopularity,
    CASE 
        WHEN s.TotalFunding > 5000000 THEN 'High Funding'
        WHEN s.TotalFunding > 1000000 THEN 'Medium Funding'
        ELSE 'Low Funding'
    END AS FundingLevel
FROM CategoryMasterLayer2 c
LEFT JOIN CategoryStatsLayer3 s ON c.CategoryID = s.CategoryID
WHERE c.IsActive = 1;
GO

-- Trend Analysis View
CREATE OR ALTER VIEW vw_FundingTrends AS
SELECT
    a.AnalysisDate,
    a.AnalysisType,
    a.TotalOpportunities,
    a.NewOpportunities,
    a.TotalFunding,
    a.AvgFunding,
    a.LargeFundingCount,
    a.MediumFundingCount,
    a.SmallFundingCount,
    a.FundingTrend,
    a.OpportunityTrend,
    a.TopFundingAgency,
    a.TopFundingCategory,
    CASE 
        WHEN a.FundingTrend > 0.10 THEN 'Strong Growth'
        WHEN a.FundingTrend BETWEEN 0.02 AND 0.10 THEN 'Moderate Growth'
        WHEN a.FundingTrend BETWEEN -0.02 AND 0.02 THEN 'Stable'
        WHEN a.FundingTrend BETWEEN -0.10 AND -0.02 THEN 'Moderate Decline'
        ELSE 'Strong Decline'
    END AS TrendCategory
FROM TrendAnalysisLayer3 a
WHERE a.IsActive = 1;
GO

-- Grant Deadline Calendar View
CREATE OR ALTER VIEW vw_GrantDeadlineCalendar AS
SELECT 
    g.GrantID,
    g.OpportunityID,
    g.Title,
    a.AgencyName,
    c.CategoryName,
    g.CloseDate,
    g.DaysUntilDeadline,
    g.EstimatedTotalFunding,
    g.AwardCeiling,
    g.Status,
    DATEPART(QUARTER, g.CloseDate) AS CloseDateQuarter,
    DATEPART(MONTH, g.CloseDate) AS CloseDateMonth,
    DATEPART(WEEK, g.CloseDate) AS CloseDateWeek,
    DATENAME(WEEKDAY, g.CloseDate) AS CloseDateWeekday
FROM CleanedGrantsLayer2 g
LEFT JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
LEFT JOIN CategoryMasterLayer2 c ON g.CategoryID = c.CategoryID
WHERE g.IsActive = 1 
AND g.CloseDate >= GETDATE()
ORDER BY g.CloseDate;
GO

-- Competition Analysis View
CREATE OR ALTER VIEW vw_CompetitionAnalysis AS
SELECT
    g.GrantID,
    g.OpportunityID,
    g.Title,
    a.AgencyName,
    c.CategoryName,
    g.ExpectedAwards,
    g.EstimatedTotalFunding,
    g.AwardCeiling,
    g.AwardFloor,
    b.ApplicantCount,
    b.PastCompetitionLevel,
    b.RecommendedFocus,
    CASE
        WHEN b.ApplicantCount = 0 OR b.ApplicantCount IS NULL THEN 'Unknown'
        WHEN b.ApplicantCount / NULLIF(g.ExpectedAwards, 0) > 10 THEN 'Very High Competition'
        WHEN b.ApplicantCount / NULLIF(g.ExpectedAwards, 0) BETWEEN 5 AND 10 THEN 'High Competition'
        WHEN b.ApplicantCount / NULLIF(g.ExpectedAwards, 0) BETWEEN 3 AND 5 THEN 'Moderate Competition'
        ELSE 'Low Competition'
    END AS CompetitionLevel
FROM CleanedGrantsLayer2 g
LEFT JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
LEFT JOIN CategoryMasterLayer2 c ON g.CategoryID = c.CategoryID
LEFT JOIN GrantBusinessViewLayer3 b ON g.GrantID = b.GrantID
WHERE g.IsActive = 1;
GO

-- Eligibility Analysis View
CREATE OR ALTER VIEW vw_EligibilityAnalysis AS
SELECT
    e.EligibilityID,
    e.EligibilityType,
    e.EligibilityDescription,
    COUNT(ge.GrantID) AS GrantCount,
    SUM(g.EstimatedTotalFunding) AS TotalFunding,
    AVG(g.AwardCeiling) AS AvgMaxAward,
    COUNT(DISTINCT g.AgencyID) AS UniqueAgencies,
    COUNT(DISTINCT g.CategoryID) AS UniqueCategories,
    COUNT(CASE WHEN g.CloseDate >= GETDATE() THEN 1 END) AS ActiveGrants
FROM EligibilityMasterLayer2 e
LEFT JOIN GrantEligibilityLayer2 ge ON e.EligibilityID = ge.EligibilityID
LEFT JOIN CleanedGrantsLayer2 g ON ge.GrantID = g.GrantID
GROUP BY e.EligibilityID, e.EligibilityType, e.EligibilityDescription
ORDER BY GrantCount DESC;
GO

-- Success Factors View
CREATE OR ALTER VIEW vw_SuccessFactors AS
SELECT
    g.GrantID,
    g.OpportunityID,
    g.Title,
    a.AgencyName,
    f.SuccessMetric,
    f.SuccessScore,
    f.KeyCompetitiveFactors,
    f.RecommendedApproach,
    f.PastSuccessTemplates,
    f.RequiredCapabilities,
    CASE
        WHEN f.SuccessScore >= 80 THEN 'Highly Favorable'
        WHEN f.SuccessScore BETWEEN 60 AND 79 THEN 'Favorable'
        WHEN f.SuccessScore BETWEEN 40 AND 59 THEN 'Moderate'
        WHEN f.SuccessScore BETWEEN 20 AND 39 THEN 'Challenging'
        ELSE 'Very Challenging'
    END AS OpportunityAssessment
FROM CleanedGrantsLayer2 g
JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
JOIN SuccessFactorsLayer3 f ON g.GrantID = f.GrantID
WHERE g.IsActive = 1
AND g.CloseDate >= GETDATE();
GO

-- Geographic Distribution View
CREATE OR ALTER VIEW vw_GeographicDistribution AS
SELECT
    gl.GeographicLevel,
    gl.RegionName,
    COUNT(g.GrantID) AS GrantCount,
    SUM(g.EstimatedTotalFunding) AS TotalFunding,
    AVG(g.AwardCeiling) AS AvgMaxAward,
    COUNT(DISTINCT g.AgencyID) AS AgencyCount,
    COUNT(DISTINCT g.CategoryID) AS CategoryCount,
    COUNT(CASE WHEN g.CloseDate >= GETDATE() THEN g.GrantID END) AS ActiveGrants
FROM GeographicCoverageMasterLayer2 gl
JOIN GrantGeographicCoverageLayer2 gg ON gl.GeographicID = gg.GeographicID
JOIN CleanedGrantsLayer2 g ON gg.GrantID = g.GrantID
GROUP BY gl.GeographicLevel, gl.RegionName
ORDER BY TotalFunding DESC;
GO
