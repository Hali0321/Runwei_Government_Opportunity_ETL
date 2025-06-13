-- ===================================
-- GRANTS.GOV API AZURE - TRIGGERS
-- ===================================

USE GrantsGovDB;
GO

-- Trigger to update DataQualityScore on insert/update
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_UpdateDataQuality')
    DROP TRIGGER trg_UpdateDataQuality;
GO

CREATE TRIGGER trg_UpdateDataQuality
ON CleanedGrantsLayer2
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @UpdatedRows TABLE (GrantID INT);
    
    -- Get inserted/updated IDs
    INSERT INTO @UpdatedRows (GrantID)
    SELECT GrantID FROM inserted;
    
    -- Update quality score
    UPDATE g
    SET DataQualityScore = dbo.CalculateDataQuality(g.GrantID)
    FROM CleanedGrantsLayer2 g
    JOIN @UpdatedRows u ON g.GrantID = u.GrantID;
END;
GO

-- Trigger to update AgencyMasterLayer2 stats on insert/update/delete in CleanedGrantsLayer2
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_UpdateAgencyStats')
    DROP TRIGGER trg_UpdateAgencyStats;
GO

CREATE TRIGGER trg_UpdateAgencyStats
ON CleanedGrantsLayer2
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @AffectedAgencies TABLE (AgencyID INT);
    
    -- Get affected agency IDs from inserted and deleted records
    INSERT INTO @AffectedAgencies (AgencyID)
    SELECT AgencyID FROM inserted
    WHERE AgencyID IS NOT NULL
    UNION
    SELECT AgencyID FROM deleted
    WHERE AgencyID IS NOT NULL;
    
    -- Update agency statistics
    UPDATE a
    SET GrantCount = (
            SELECT COUNT(*) 
            FROM CleanedGrantsLayer2 
            WHERE AgencyID = a.AgencyID AND IsActive = 1
        ),
        TotalFunding = (
            SELECT ISNULL(SUM(AwardCeiling), 0) 
            FROM CleanedGrantsLayer2 
            WHERE AgencyID = a.AgencyID AND IsActive = 1
        ),
        ActiveGrantsCount = (
            SELECT COUNT(*) 
            FROM CleanedGrantsLayer2 
            WHERE AgencyID = a.AgencyID AND IsActive = 1 AND CloseDate >= GETDATE()
        ),
        AvgFundingAmount = (
            SELECT ISNULL(AVG(AwardCeiling), 0) 
            FROM CleanedGrantsLayer2 
            WHERE AgencyID = a.AgencyID AND IsActive = 1
        ),
        LastUpdated = GETUTCDATE()
    FROM AgencyMasterLayer2 a
    JOIN @AffectedAgencies aa ON a.AgencyID = aa.AgencyID;
END;
GO

-- Trigger to update CategoryMasterLayer2 stats on insert/update/delete in CleanedGrantsLayer2
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_UpdateCategoryStats')
    DROP TRIGGER trg_UpdateCategoryStats;
GO

CREATE TRIGGER trg_UpdateCategoryStats
ON CleanedGrantsLayer2
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @AffectedCategories TABLE (CategoryID INT);
    
    -- Get affected category IDs from inserted and deleted records
    INSERT INTO @AffectedCategories (CategoryID)
    SELECT CategoryID FROM inserted
    WHERE CategoryID IS NOT NULL
    UNION
    SELECT CategoryID FROM deleted
    WHERE CategoryID IS NOT NULL;
    
    -- Update category statistics
    UPDATE c
    SET GrantCount = (
            SELECT COUNT(*) 
            FROM CleanedGrantsLayer2 
            WHERE CategoryID = c.CategoryID AND IsActive = 1
        ),
        TotalFunding = (
            SELECT ISNULL(SUM(AwardCeiling), 0) 
            FROM CleanedGrantsLayer2 
            WHERE CategoryID = c.CategoryID AND IsActive = 1
        ),
        ActiveGrantsCount = (
            SELECT COUNT(*) 
            FROM CleanedGrantsLayer2 
            WHERE CategoryID = c.CategoryID AND IsActive = 1 AND CloseDate >= GETDATE()
        ),
        AvgFunding = (
            SELECT ISNULL(AVG(AwardCeiling), 0) 
            FROM CleanedGrantsLayer2 
            WHERE CategoryID = c.CategoryID AND IsActive = 1
        ),
        LastUpdated = GETUTCDATE()
    FROM CategoryMasterLayer2 c
    JOIN @AffectedCategories ac ON c.CategoryID = ac.CategoryID;
END;
GO

PRINT '✅ Triggers created successfully';
GO
