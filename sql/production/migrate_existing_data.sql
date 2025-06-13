-- ===================================
-- PRODUCTION MIGRATION SCRIPT - NTEXT COMPATIBLE
-- Migrates 3,236 grants with NTEXT handling and duplicate prevention
-- ===================================

SET NOCOUNT ON;
GO

PRINT '===============================================';
PRINT 'Starting PRODUCTION migration - ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT 'Source: 3,236 grants ready for migration';
PRINT 'NTEXT compatible version with proper data type handling';
PRINT '===============================================';

BEGIN TRY
    -- Log migration start
    EXEC LogDeploymentStep 'Production Migration', 'Starting', 'NTEXT-compatible migration of 3,236 grants';
    
    -- Step 1: Clear existing migrated data for clean migration
    DELETE FROM GrantEligibilityLayer2;
    DELETE FROM GrantBusinessViewLayer3; 
    DELETE FROM AgencyStatsLayer3;
    DELETE FROM CleanedGrantsLayer2;
    PRINT 'Cleared existing migration data for fresh start';
    
    -- Keep only default agencies, remove duplicates from previous failed attempts
    DELETE FROM AgencyMasterLayer2 WHERE AgencyCode NOT IN ('HHS', 'NIH', 'NSF', 'ED', 'DOE', 'USDA', 'NASA', 'EPA');
    DELETE FROM CategoryMasterLayer2 WHERE CategoryID > 8; -- Keep only the default 8 categories
    
    -- Step 2: Add agencies from source data with duplicate handling
    EXEC LogDeploymentStep 'Agency Migration', 'Starting', 'Adding agencies with duplicate prevention';
    
    INSERT INTO AgencyMasterLayer2 (AgencyCode, AgencyName, IsActive, DataQualityScore, CreatedDate)
    SELECT DISTINCT 
        LEFT(LTRIM(RTRIM(ISNULL(AgencyCode, 'UNKNOWN'))), 100) as AgencyCode,
        LEFT(LTRIM(RTRIM(ISNULL(AgencyName, 'Unknown Agency'))), 500) as AgencyName,
        1,
        80,
        GETDATE()
    FROM Grants 
    WHERE AgencyCode IS NOT NULL 
        AND AgencyName IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM AgencyMasterLayer2 a 
            WHERE a.AgencyCode = LEFT(LTRIM(RTRIM(ISNULL(Grants.AgencyCode, 'UNKNOWN'))), 100)
        );

    DECLARE @NewAgencies INT = @@ROWCOUNT;
    PRINT 'Added ' + CAST(@NewAgencies AS NVARCHAR(10)) + ' new agencies';

    -- Step 3: Add categories with duplicate handling  
    INSERT INTO CategoryMasterLayer2 (CategoryName, CategoryGroup, IsActive, CreatedDate)
    SELECT DISTINCT 
        LEFT(LTRIM(RTRIM(ISNULL(Category, 'General'))), 255) as CategoryName,
        CASE 
            WHEN Category LIKE '%Science%' OR Category LIKE '%Research%' THEN 'STEM'
            WHEN Category LIKE '%Education%' OR Category LIKE '%Training%' THEN 'Education and Training'
            WHEN Category LIKE '%Health%' OR Category LIKE '%Medical%' THEN 'Medical and Health Sciences'
            WHEN Category LIKE '%Environment%' THEN 'Environmental Sciences'
            WHEN Category LIKE '%Agriculture%' OR Category LIKE '%Food%' THEN 'Food and Agriculture'
            WHEN Category LIKE '%Energy%' THEN 'Energy and Environment'
            WHEN Category LIKE '%Transportation%' THEN 'Transportation and Infrastructure'
            WHEN Category LIKE '%Housing%' OR Category LIKE '%Community%' THEN 'Housing and Community Development'
            ELSE 'General'
        END as CategoryGroup,
        1,
        GETDATE()
    FROM Grants 
    WHERE Category IS NOT NULL
        AND LTRIM(RTRIM(Category)) != ''
        AND NOT EXISTS (
            SELECT 1 FROM CategoryMasterLayer2 c 
            WHERE c.CategoryName = LEFT(LTRIM(RTRIM(ISNULL(Grants.Category, 'General'))), 255)
        );

    DECLARE @NewCategories INT = @@ROWCOUNT;
    PRINT 'Added ' + CAST(@NewCategories AS NVARCHAR(10)) + ' new categories';

    -- Step 4: Migrate grants with NTEXT-safe duplicate handling
    EXEC LogDeploymentStep 'Grant Migration', 'Starting', 'Migrating grants with NTEXT-safe deduplication';
    
    WITH DeduplicatedGrants AS (
        SELECT 
            ROW_NUMBER() OVER (
                PARTITION BY ISNULL(LTRIM(RTRIM(RowKey)), CAST(ID AS NVARCHAR(255))) 
                ORDER BY 
                    CASE WHEN Title IS NOT NULL AND LTRIM(RTRIM(Title)) != '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN Description IS NOT NULL AND DATALENGTH(Description) > 100 THEN 1 ELSE 0 END DESC,
                    CASE WHEN EstimatedTotalFunding IS NOT NULL AND ISNUMERIC(EstimatedTotalFunding) = 1 THEN 1 ELSE 0 END DESC,
                    ID DESC
            ) as RowNum,
            ISNULL(LTRIM(RTRIM(RowKey)), CAST(ID AS NVARCHAR(255))) as OpportunityID,
            LTRIM(RTRIM(ISNULL(Title, 'Grant Opportunity ' + CAST(ID AS NVARCHAR(10))))) as Title,
            -- Safe NTEXT to NVARCHAR conversion
            CASE 
                WHEN Description IS NOT NULL THEN CAST(Description AS NVARCHAR(4000))
                ELSE ''
            END as Description,
            TRY_CAST(AwardCeiling AS DECIMAL(18,2)) as AwardCeiling,
            TRY_CAST(AwardFloor AS DECIMAL(18,2)) as AwardFloor,
            TRY_CAST(EstimatedTotalFunding AS DECIMAL(18,2)) as EstimatedTotalFunding,
            TRY_CAST(ExpectedAwards AS INT) as ExpectedAwards,
            TRY_CAST(PostedDate AS DATETIME) as PostedDate,
            TRY_CAST(CloseDate AS DATETIME) as CloseDate,
            -- Safe NTEXT handling for EligibleApplicants
            CASE 
                WHEN EligibleApplicants IS NOT NULL THEN LEFT(CAST(EligibleApplicants AS NVARCHAR(4000)), 4000)
                ELSE ''
            END as EligibilityFullText,
            LEFT(ISNULL(CFDANumbers, ''), 500) as CFDANumbers,
            LEFT(ISNULL(AdditionalInfoURL, ''), 1000) as AdditionalInfoURL,
            LEFT(ISNULL(GrantorEmail, ''), 255) as ContactEmail,
            -- Safe keyword handling
            CASE 
                WHEN Category IS NOT NULL AND FundingType IS NOT NULL 
                THEN LEFT(LTRIM(RTRIM(Category)) + '; ' + LTRIM(RTRIM(FundingType)), 500)
                WHEN Category IS NOT NULL 
                THEN LEFT(LTRIM(RTRIM(Category)), 500)
                WHEN FundingType IS NOT NULL 
                THEN LEFT(LTRIM(RTRIM(FundingType)), 500)
                ELSE NULL
            END as Keywords,
            AgencyCode,
            AgencyName,
            Category
        FROM Grants
        WHERE Title IS NOT NULL 
            AND LTRIM(RTRIM(Title)) != ''
    )
    INSERT INTO CleanedGrantsLayer2 (
        OpportunityID, Title, Description, AwardCeiling, AwardFloor, 
        EstimatedTotalFunding, ExpectedAwards, CFDANumbers, PostedDate, CloseDate,
        Status, EligibilityFullText, Keywords, AdditionalInfoURL, ContactEmail,
        AgencyID, CategoryID, IsActive, DataQualityScore, CreatedDate
    )
    SELECT 
        d.OpportunityID,
        d.Title,
        d.Description,
        d.AwardCeiling,
        d.AwardFloor,
        d.EstimatedTotalFunding,
        d.ExpectedAwards,
        d.CFDANumbers,
        d.PostedDate,
        d.CloseDate,
        CASE 
            WHEN d.CloseDate IS NOT NULL AND d.CloseDate < GETDATE() THEN 'Closed'
            WHEN d.CloseDate IS NOT NULL AND DATEDIFF(day, GETDATE(), d.CloseDate) <= 7 THEN 'Closing Soon'
            WHEN d.CloseDate IS NOT NULL AND DATEDIFF(day, GETDATE(), d.CloseDate) <= 30 THEN 'Closing This Month'
            WHEN d.PostedDate > GETDATE() THEN 'Upcoming'
            ELSE 'Open'
        END as Status,
        d.EligibilityFullText,
        d.Keywords,
        d.AdditionalInfoURL,
        d.ContactEmail,
        -- Safe agency mapping
        COALESCE(
            (SELECT TOP 1 AgencyID FROM AgencyMasterLayer2 WHERE AgencyCode = LEFT(LTRIM(RTRIM(ISNULL(d.AgencyCode, 'UNKNOWN'))), 100)),
            (SELECT TOP 1 AgencyID FROM AgencyMasterLayer2 WHERE AgencyName = LEFT(LTRIM(RTRIM(ISNULL(d.AgencyName, 'Unknown Agency'))), 500)),
            (SELECT MIN(AgencyID) FROM AgencyMasterLayer2)
        ) as AgencyID,
        -- Safe category mapping
        COALESCE(
            (SELECT TOP 1 CategoryID FROM CategoryMasterLayer2 WHERE CategoryName = LEFT(LTRIM(RTRIM(ISNULL(d.Category, 'General'))), 255)),
            (SELECT MIN(CategoryID) FROM CategoryMasterLayer2)
        ) as CategoryID,
        1, -- IsActive
        -- NTEXT-safe data quality scoring
        CASE 
            WHEN d.Title IS NOT NULL AND d.Description IS NOT NULL AND d.EstimatedTotalFunding IS NOT NULL 
                AND d.PostedDate IS NOT NULL AND d.CloseDate IS NOT NULL THEN 9.5
            WHEN d.Title IS NOT NULL AND d.Description IS NOT NULL AND d.EstimatedTotalFunding IS NOT NULL THEN 8.5
            WHEN d.Title IS NOT NULL AND d.Description IS NOT NULL AND d.Description != '' THEN 7.5
            WHEN d.Title IS NOT NULL THEN 6.5
            ELSE 5.0
        END,
        GETDATE()
    FROM DeduplicatedGrants d
    WHERE d.RowNum = 1; -- Only take the best record for each OpportunityID

    DECLARE @MigratedGrants INT = @@ROWCOUNT;
    PRINT 'Successfully migrated ' + CAST(@MigratedGrants AS NVARCHAR(10)) + ' unique grants';

    -- Step 5: Create business intelligence insights
    INSERT INTO GrantBusinessViewLayer3 (GrantID, CompetitionLevel, FundingTier, RecommendedFocus, CreatedDate)
    SELECT 
        g.GrantID,
        CASE 
            WHEN g.AwardCeiling >= 1000000 THEN 'High'
            WHEN g.AwardCeiling >= 100000 THEN 'Medium'
            ELSE 'Low'
        END as CompetitionLevel,
        CASE 
            WHEN g.AwardCeiling >= 5000000 THEN 'Tier 1 - Major Program'
            WHEN g.AwardCeiling >= 1000000 THEN 'Tier 2 - Significant Grant'
            WHEN g.AwardCeiling >= 250000 THEN 'Tier 3 - Standard Grant'
            WHEN g.AwardCeiling >= 50000 THEN 'Tier 4 - Small Grant'
            ELSE 'Tier 5 - Micro Grant'
        END as FundingTier,
        CASE 
            WHEN g.DaysUntilDeadline <= 7 AND g.DaysUntilDeadline >= 0 THEN 'URGENT: Apply within 1 week'
            WHEN g.DaysUntilDeadline <= 30 AND g.DaysUntilDeadline >= 0 THEN 'HIGH PRIORITY: Apply within 30 days'
            WHEN g.DaysUntilDeadline <= 60 AND g.DaysUntilDeadline >= 0 THEN 'MEDIUM PRIORITY: Apply within 60 days'
            WHEN g.DaysUntilDeadline > 60 THEN 'PLAN AHEAD: Good for future planning'
            ELSE 'OPEN: No specific deadline'
        END as RecommendedFocus,
        GETDATE()
    FROM CleanedGrantsLayer2 g;

    DECLARE @BusinessInsights INT = @@ROWCOUNT;
    PRINT 'Created ' + CAST(@BusinessInsights AS NVARCHAR(10)) + ' business intelligence records';

    -- Step 6: Update agency statistics
    INSERT INTO AgencyStatsLayer3 (
        AgencyID, TotalGrants, ActiveGrants, ClosedGrants, 
        TotalFunding, AvgFunding, CategoryCount, LastCalculated
    )
    SELECT 
        a.AgencyID,
        COUNT(g.GrantID) as TotalGrants,
        COUNT(CASE WHEN g.Status IN ('Open', 'Closing Soon', 'Closing This Month') THEN 1 END) as ActiveGrants,
        COUNT(CASE WHEN g.Status = 'Closed' THEN 1 END) as ClosedGrants,
        ISNULL(SUM(g.EstimatedTotalFunding), 0) as TotalFunding,
        ISNULL(AVG(g.EstimatedTotalFunding), 0) as AvgFunding,
        COUNT(DISTINCT g.CategoryID) as CategoryCount,
        GETDATE()
    FROM AgencyMasterLayer2 a
    LEFT JOIN CleanedGrantsLayer2 g ON a.AgencyID = g.AgencyID
    GROUP BY a.AgencyID;

    PRINT 'Updated agency statistics';

    -- Final validation and summary
    DECLARE @FinalGrants INT, @FinalAgencies INT, @FinalCategories INT;
    SELECT @FinalGrants = COUNT(*) FROM CleanedGrantsLayer2;
    SELECT @FinalAgencies = COUNT(*) FROM AgencyMasterLayer2;
    SELECT @FinalCategories = COUNT(*) FROM CategoryMasterLayer2;
    
    DECLARE @SummaryMsg NVARCHAR(500) = 
        'NTEXT MIGRATION COMPLETED SUCCESSFULLY! ' +
        'Grants: ' + CAST(@FinalGrants AS NVARCHAR(10)) + 
        ', Agencies: ' + CAST(@FinalAgencies AS NVARCHAR(10)) + 
        ', Categories: ' + CAST(@FinalCategories AS NVARCHAR(10));
    
    EXEC LogDeploymentStep 'Production Migration', 'SUCCESS', @SummaryMsg;
    PRINT @SummaryMsg;

    -- Validate the API view is populated
    DECLARE @APIRecords INT;
    SELECT @APIRecords = COUNT(*) FROM api.vw_GrantSummary;
    PRINT 'API View populated with ' + CAST(@APIRecords AS NVARCHAR(10)) + ' records';

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    
    DECLARE @FullErrorMessage NVARCHAR(4000) = 
        'NTEXT MIGRATION ERROR at Line ' + CAST(@ErrorLine AS NVARCHAR(10)) + ': ' + @ErrorMessage;
    
    EXEC LogDeploymentStep 'Production Migration', 'FAILED', @FullErrorMessage;
    PRINT @FullErrorMessage;
    RAISERROR(@FullErrorMessage, 16, 1);
END CATCH;

PRINT '===============================================';
PRINT 'NTEXT MIGRATION completed at ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT 'Ready for API deployment and business presentation!';
PRINT '===============================================';
GO