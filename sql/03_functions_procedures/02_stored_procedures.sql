-- ===================================
-- GRANTS.GOV API AZURE - STORED PROCEDURES
-- ===================================

USE GrantsGovDB;
GO

-- Refresh Business Views
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'RefreshBusinessViews')
    DROP PROCEDURE RefreshBusinessViews;
GO

CREATE PROCEDURE RefreshBusinessViews
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Refresh Grant Business View
    MERGE GrantBusinessViewLayer3 AS target
    USING (
        SELECT 
            g.GrantID,
            g.OpportunityID,
            g.Title,
            a.AgencyName,
            c.CategoryName,
            g.Status,
            CASE 
                WHEN g.DaysUntilDeadline <= 7 AND g.DaysUntilDeadline >= 0 THEN 'Critical'
                WHEN g.DaysUntilDeadline <= 30 AND g.DaysUntilDeadline >= 0 THEN 'High'
                WHEN g.DaysUntilDeadline <= 60 AND g.DaysUntilDeadline >= 0 THEN 'Medium'
                ELSE 'Low'
            END as UrgencyLevel,
            CASE 
                WHEN g.AwardCeiling >= 1000000 THEN 'Large'
                WHEN g.AwardCeiling >= 500000 THEN 'Medium'
                WHEN g.AwardCeiling >= 100000 THEN 'Small'
                ELSE 'Micro'
            END as FundingTier,
            CASE 
                WHEN g.ExpectedAwards <= 5 AND g.AwardCeiling >= 500000 THEN 'High'
                WHEN g.ExpectedAwards <= 20 THEN 'Medium'
                ELSE 'Low'
            END as CompetitionLevel,
            g.DaysUntilDeadline,
            g.AwardCeiling,
            g.AwardFloor,
            g.EstimatedTotalFunding,
            g.ExpectedAwards,
            g.PostedDate,
            g.CloseDate,
            g.GrantsGovURL,
            g.AdditionalInfoURL,
            g.ContactEmail
        FROM CleanedGrantsLayer2 g
        LEFT JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
        LEFT JOIN CategoryMasterLayer2 c ON g.CategoryID = c.CategoryID
        WHERE g.IsActive = 1
    ) AS source ON target.GrantID = source.GrantID
    
    WHEN MATCHED THEN
        UPDATE SET
            OpportunityID = source.OpportunityID,
            Title = source.Title,
            AgencyName = source.AgencyName,
            CategoryName = source.CategoryName,
            Status = source.Status,
            UrgencyLevel = source.UrgencyLevel,
            FundingTier = source.FundingTier,
            CompetitionLevel = source.CompetitionLevel,
            DaysUntilDeadline = source.DaysUntilDeadline,
            AwardCeiling = source.AwardCeiling,
            AwardFloor = source.AwardFloor,
            EstimatedTotalFunding = source.EstimatedTotalFunding,
            ExpectedAwards = source.ExpectedAwards,
            PostedDate = source.PostedDate,
            CloseDate = source.CloseDate,
            GrantsGovURL = source.GrantsGovURL,
            AdditionalInfoURL = source.AdditionalInfoURL,
            ContactEmail = source.ContactEmail,
            RefreshDate = GETUTCDATE()
    
    WHEN NOT MATCHED THEN
        INSERT (GrantID, OpportunityID, Title, AgencyName, CategoryName, Status, 
                UrgencyLevel, FundingTier, CompetitionLevel, DaysUntilDeadline,
                AwardCeiling, AwardFloor, EstimatedTotalFunding, ExpectedAwards,
                PostedDate, CloseDate, GrantsGovURL, AdditionalInfoURL, ContactEmail)
        VALUES (source.GrantID, source.OpportunityID, source.Title, source.AgencyName, 
                source.CategoryName, source.Status, source.UrgencyLevel, source.FundingTier,
                source.CompetitionLevel, source.DaysUntilDeadline, source.AwardCeiling,
                source.AwardFloor, source.EstimatedTotalFunding, source.ExpectedAwards,
                source.PostedDate, source.CloseDate, source.GrantsGovURL, 
                source.AdditionalInfoURL, source.ContactEmail);
    
    -- Refresh Agency Stats
    MERGE AgencyStatsLayer3 AS target
    USING (
        SELECT 
            a.AgencyID,
            a.AgencyName,
            COUNT(g.GrantID) as TotalGrants,
            SUM(CASE WHEN g.CloseDate >= GETDATE() THEN 1 ELSE 0 END) as ActiveGrants,
            SUM(CASE WHEN g.CloseDate < GETDATE() THEN 1 ELSE 0 END) as ClosedGrants,
            SUM(CASE WHEN g.PostedDate > GETDATE() THEN 1 ELSE 0 END) as UpcomingGrants,
            ISNULL(SUM(g.AwardCeiling), 0) as TotalFunding,
            ISNULL(AVG(g.AwardCeiling), 0) as AvgFunding,
            ISNULL(MIN(g.AwardCeiling), 0) as MinFunding,
            ISNULL(MAX(g.AwardCeiling), 0) as MaxFunding,
            COUNT(DISTINCT g.CategoryID) as CategoryCount,
            ISNULL(AVG(DATEDIFF(day, g.PostedDate, g.CloseDate)), 0) as AvgApplicationPeriod
        FROM AgencyMasterLayer2 a
        LEFT JOIN CleanedGrantsLayer2 g ON a.AgencyID = g.AgencyID AND g.IsActive = 1
        GROUP BY a.AgencyID, a.AgencyName
    ) AS source ON target.AgencyID = source.AgencyID
    
    WHEN MATCHED THEN
        UPDATE SET
            AgencyName = source.AgencyName,
            TotalGrants = source.TotalGrants,
            ActiveGrants = source.ActiveGrants,
            ClosedGrants = source.ClosedGrants,
            UpcomingGrants = source.UpcomingGrants,
            TotalFunding = source.TotalFunding,
            AvgFunding = source.AvgFunding,
            MinFunding = source.MinFunding,
            MaxFunding = source.MaxFunding,
            CategoryCount = source.CategoryCount,
            AvgApplicationPeriod = source.AvgApplicationPeriod,
            LastUpdated = GETUTCDATE()
    
    WHEN NOT MATCHED THEN
        INSERT (AgencyID, AgencyName, TotalGrants, ActiveGrants, ClosedGrants, UpcomingGrants,
                TotalFunding, AvgFunding, MinFunding, MaxFunding, CategoryCount, AvgApplicationPeriod)
        VALUES (source.AgencyID, source.AgencyName, source.TotalGrants, source.ActiveGrants,
                source.ClosedGrants, source.UpcomingGrants, source.TotalFunding, source.AvgFunding,
                source.MinFunding, source.MaxFunding, source.CategoryCount, source.AvgApplicationPeriod);
    
    -- Refresh Category Stats
    MERGE CategoryStatsLayer3 AS target
    USING (
        SELECT 
            c.CategoryID,
            c.CategoryName,
            c.CategoryGroup,
            COUNT(g.GrantID) as TotalGrants,
            SUM(CASE WHEN g.CloseDate >= GETDATE() THEN 1 ELSE 0 END) as ActiveGrants,
            SUM(CASE WHEN g.CloseDate < GETDATE() THEN 1 ELSE 0 END) as CompletedGrants,
            ISNULL(AVG(g.AwardCeiling), 0) as AvgFunding,
            ISNULL(SUM(g.AwardCeiling), 0) as TotalFunding,
            COUNT(DISTINCT g.AgencyID) as AgencyCount
        FROM CategoryMasterLayer2 c
        LEFT JOIN CleanedGrantsLayer2 g ON c.CategoryID = g.CategoryID AND g.IsActive = 1
        GROUP BY c.CategoryID, c.CategoryName, c.CategoryGroup
    ) AS source ON target.CategoryID = source.CategoryID
    
    WHEN MATCHED THEN
        UPDATE SET
            CategoryName = source.CategoryName,
            CategoryGroup = source.CategoryGroup,
            TotalGrants = source.TotalGrants,
            ActiveGrants = source.ActiveGrants,
            CompletedGrants = source.CompletedGrants,
            AvgFunding = source.AvgFunding,
            TotalFunding = source.TotalFunding,
            AgencyCount = source.AgencyCount,
            LastUpdated = GETUTCDATE()
    
    WHEN NOT MATCHED THEN
        INSERT (CategoryID, CategoryName, CategoryGroup, TotalGrants, ActiveGrants, 
                CompletedGrants, AvgFunding, TotalFunding, AgencyCount)
        VALUES (source.CategoryID, source.CategoryName, source.CategoryGroup, source.TotalGrants, 
                source.ActiveGrants, source.CompletedGrants, source.AvgFunding, source.TotalFunding,
                source.AgencyCount);
                
    PRINT '✅ Business views refreshed successfully';
END;
GO

-- Process Raw Grant Data
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'ProcessRawGrantData')
    DROP PROCEDURE ProcessRawGrantData;
GO

CREATE PROCEDURE ProcessRawGrantData
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessedCount INT = 0;
    DECLARE @ErrorCount INT = 0;
    
    -- Step 1: Update or insert into AgencyMasterLayer2
    MERGE AgencyMasterLayer2 AS target
    USING (
        SELECT DISTINCT
            AgencyName,
            AgencyCode
        FROM RawGrantsLayer1
        WHERE ProcessingStatus = 'Raw'
        AND AgencyName IS NOT NULL
    ) AS source ON target.AgencyName = source.AgencyName
    
    WHEN MATCHED THEN
        UPDATE SET 
            AgencyCode = ISNULL(target.AgencyCode, source.AgencyCode),
            LastUpdated = GETUTCDATE()
    
    WHEN NOT MATCHED THEN
        INSERT (AgencyName, AgencyCode)
        VALUES (source.AgencyName, source.AgencyCode);
    
    PRINT '✅ Agencies processed';
    
    -- Step 2: Update or insert into CategoryMasterLayer2
    MERGE CategoryMasterLayer2 AS target
    USING (
        SELECT DISTINCT
            CategoryOfFundingActivity as CategoryName
        FROM RawGrantsLayer1
        WHERE ProcessingStatus = 'Raw'
        AND CategoryOfFundingActivity IS NOT NULL
    ) AS source ON target.CategoryName = source.CategoryName
    
    WHEN NOT MATCHED THEN
        INSERT (CategoryName, CategoryGroup)
        VALUES (source.CategoryName, 'Auto-Categorized');
    
    PRINT '✅ Categories processed';
    
    -- Step 3: Process grants
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Insert into CleanedGrantsLayer2
        INSERT INTO CleanedGrantsLayer2 (
            OpportunityID,
            Title,
            AgencyID,
            CategoryID,
            Description,
            PostedDate,
            CloseDate,
            AwardCeiling,
            AwardFloor,
            EstimatedTotalFunding,
            ExpectedAwards,
            InstrumentType,
            FundingActivity,
            ContactEmail,
            AdditionalInfoURL,
            OriginalRowID
        )
        SELECT 
            r.OpportunityNumber,
            r.OpportunityTitle,
            a.AgencyID,
            c.CategoryID,
            r.FundingDescription,
            CONVERT(DATE, CASE WHEN ISDATE(r.PostedDate) = 1 THEN r.PostedDate ELSE NULL END),
            CONVERT(DATE, CASE WHEN ISDATE(r.CloseDate) = 1 THEN r.CloseDate ELSE NULL END),
            CONVERT(MONEY, CASE WHEN ISNUMERIC(REPLACE(r.AwardCeiling, ',', '')) = 1 
                            THEN REPLACE(r.AwardCeiling, ',', '') ELSE NULL END),
            CONVERT(MONEY, CASE WHEN ISNUMERIC(REPLACE(r.AwardFloor, ',', '')) = 1 
                            THEN REPLACE(r.AwardFloor, ',', '') ELSE NULL END),
            CONVERT(MONEY, CASE WHEN ISNUMERIC(REPLACE(r.EstimatedTotalFunding, ',', '')) = 1 
                            THEN REPLACE(r.EstimatedTotalFunding, ',', '') ELSE NULL END),
            CONVERT(INT, CASE WHEN ISNUMERIC(r.ExpectedNumberOfAwards) = 1 
                            THEN r.ExpectedNumberOfAwards ELSE NULL END),
            r.FundingInstrumentType,
            r.CategoryOfFundingActivity,
            r.GrantorContactEmail,
            r.LinkToAdditionalInformation,
            r.ID
        FROM RawGrantsLayer1 r
        LEFT JOIN AgencyMasterLayer2 a ON r.AgencyName = a.AgencyName
        LEFT JOIN CategoryMasterLayer2 c ON r.CategoryOfFundingActivity = c.CategoryName
        WHERE r.ProcessingStatus = 'Raw'
        AND r.OpportunityNumber IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM CleanedGrantsLayer2 
            WHERE OpportunityID = r.OpportunityNumber
        );
        
        SET @ProcessedCount = @@ROWCOUNT;
        
        -- Update ProcessingStatus for processed records
        UPDATE RawGrantsLayer1
        SET ProcessingStatus = 'Processed',
            ValidationErrors = NULL
        WHERE ProcessingStatus = 'Raw'
        AND OpportunityNumber IN (
            SELECT OpportunityID FROM CleanedGrantsLayer2
        );
        
        -- Refresh business views
        EXEC RefreshBusinessViews;
        
        COMMIT TRANSACTION;
        
        PRINT 'Successfully processed ' + CAST(@ProcessedCount AS NVARCHAR) + ' grants';
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        SET @ErrorCount = @ErrorCount + 1;
        
        PRINT 'Error processing grants: ' + ERROR_MESSAGE();
        
        -- Log the error
        INSERT INTO dbo.ErrorLog (
            ErrorMessage,
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine
        )
        SELECT
            ERROR_MESSAGE(),
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE();
    END CATCH;
    
    SELECT @ProcessedCount AS ProcessedCount, @ErrorCount AS ErrorCount;
END;
GO

-- Create error log table if it doesn't exist
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ErrorLog' AND xtype='U')
CREATE TABLE ErrorLog (
    ErrorID INT IDENTITY(1,1) PRIMARY KEY,
    ErrorMessage NVARCHAR(4000),
    ErrorNumber INT,
    ErrorSeverity INT,
    ErrorState INT,
    ErrorProcedure NVARCHAR(128),
    ErrorLine INT,
    ErrorTime DATETIME2 DEFAULT GETUTCDATE()
);

PRINT '✅ Stored procedures created successfully';
GO
