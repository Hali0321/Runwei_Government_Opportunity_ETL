-- ===================================
-- GRANTS.GOV API AZURE - API VIEWS
-- ===================================

USE GrantsGovDB;
GO

-- API Grant Summary View - Optimized for API consumption with minimal fields
CREATE OR ALTER VIEW api.vw_GrantSummary AS
SELECT 
    g.GrantID,
    g.OpportunityID,
    g.Title,
    a.AgencyName,
    c.CategoryName,
    g.AwardCeiling,
    g.EstimatedTotalFunding,
    g.PostedDate,
    g.CloseDate,
    g.DaysUntilDeadline,
    g.Status,
    g.GrantsGovURL
FROM CleanedGrantsLayer2 g
LEFT JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
LEFT JOIN CategoryMasterLayer2 c ON g.CategoryID = c.CategoryID
WHERE g.IsActive = 1;
GO

-- API Search View - Optimized for search functionality
CREATE OR ALTER VIEW api.vw_GrantSearch AS
SELECT 
    g.GrantID,
    g.OpportunityID,
    g.Title,
    a.AgencyName,
    a.AgencyCode,
    c.CategoryName,
    c.CategoryGroup,
    g.AwardCeiling,
    g.AwardFloor,
    g.EstimatedTotalFunding,
    g.ExpectedAwards,
    g.CloseDate,
    g.Status,
    g.Keywords,
    g.Description,
    g.EligibilityFullText
FROM CleanedGrantsLayer2 g
LEFT JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
LEFT JOIN CategoryMasterLayer2 c ON g.CategoryID = c.CategoryID
WHERE g.IsActive = 1;
GO

-- API Grant Detail View - Complete grant details for single grant lookup
CREATE OR ALTER VIEW api.vw_GrantDetail AS
SELECT 
    g.GrantID,
    g.OpportunityID,
    g.Title,
    g.Description,
    a.AgencyName,
    a.AgencyCode,
    c.CategoryName,
    g.AwardCeiling,
    g.AwardFloor,
    g.EstimatedTotalFunding,
    g.ExpectedAwards,
    g.CFDANumbers,
    g.PostedDate,
    g.CloseDate,
    g.DaysUntilDeadline,
    g.Status,
    g.EligibilityFullText,
    g.GrantsGovURL,
    g.AdditionalInfoURL,
    g.ContactEmail,
    g.ContactPhone,
    g.LastUpdated,
    g.DataQualityScore
FROM CleanedGrantsLayer2 g
LEFT JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
LEFT JOIN CategoryMasterLayer2 c ON g.CategoryID = c.CategoryID;
GO

-- API Agency List View - Simplified agency list for dropdowns
CREATE OR ALTER VIEW api.vw_AgencyList AS
SELECT 
    a.AgencyID,
    a.AgencyCode,
    a.AgencyName,
    a.ParentAgency,
    a.IsActive,
    COUNT(g.GrantID) AS ActiveGrantCount
FROM AgencyMasterLayer2 a
LEFT JOIN CleanedGrantsLayer2 g ON a.AgencyID = g.AgencyID AND g.IsActive = 1 AND g.CloseDate >= GETDATE()
GROUP BY a.AgencyID, a.AgencyCode, a.AgencyName, a.ParentAgency, a.IsActive;
GO

-- API Category List View - Simplified category list for dropdowns
CREATE OR ALTER VIEW api.vw_CategoryList AS
SELECT 
    c.CategoryID,
    c.CategoryName,
    c.CategoryGroup,
    c.IsActive,
    COUNT(g.GrantID) AS ActiveGrantCount
FROM CategoryMasterLayer2 c
LEFT JOIN CleanedGrantsLayer2 g ON c.CategoryID = g.CategoryID AND g.IsActive = 1 AND g.CloseDate >= GETDATE()
GROUP BY c.CategoryID, c.CategoryName, c.CategoryGroup, c.IsActive;
GO

-- API Eligibility List View - For filtering by eligibility
CREATE OR ALTER VIEW api.vw_EligibilityList AS
SELECT 
    e.EligibilityID,
    e.EligibilityType,
    e.EligibilityDescription,
    COUNT(ge.GrantID) AS GrantCount
FROM EligibilityMasterLayer2 e
LEFT JOIN GrantEligibilityLayer2 ge ON e.EligibilityID = ge.EligibilityID
LEFT JOIN CleanedGrantsLayer2 g ON ge.GrantID = g.GrantID AND g.IsActive = 1 AND g.CloseDate >= GETDATE()
GROUP BY e.EligibilityID, e.EligibilityType, e.EligibilityDescription;
GO

-- API Dashboard Summary - For quick dashboard metrics
CREATE OR ALTER VIEW api.vw_DashboardSummary AS
SELECT
    COUNT(CASE WHEN g.CloseDate >= GETDATE() THEN g.GrantID END) AS ActiveGrantCount,
    COUNT(CASE WHEN g.DaysUntilDeadline <= 7 AND g.CloseDate >= GETDATE() THEN g.GrantID END) AS ClosingSoonCount,
    COUNT(CASE WHEN g.AwardCeiling >= 1000000 AND g.CloseDate >= GETDATE() THEN g.GrantID END) AS LargeGrantCount,
    SUM(CASE WHEN g.CloseDate >= GETDATE() THEN g.EstimatedTotalFunding ELSE 0 END) AS TotalActiveFunding,
    COUNT(DISTINCT CASE WHEN g.CloseDate >= GETDATE() THEN g.AgencyID END) AS ActiveAgencyCount,
    COUNT(DISTINCT CASE WHEN g.CloseDate >= GETDATE() THEN g.CategoryID END) AS ActiveCategoryCount
FROM CleanedGrantsLayer2 g
WHERE g.IsActive = 1;
GO