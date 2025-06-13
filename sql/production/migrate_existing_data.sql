-- ===================================
-- DATA MIGRATION SCRIPT - BULLETPROOF VERSION
-- Handles NTEXT data types and all edge cases
-- ===================================

SET NOCOUNT ON;
GO

PRINT '===============================================';
PRINT 'Starting bulletproof data migration from existing Grants table';
PRINT 'Migration started at ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '===============================================';
GO

BEGIN TRY
    -- Log migration start
    EXEC LogDeploymentStep 'Data Migration', 'Starting', 'Bulletproof migration with NTEXT handling';
    
    -- Step 0: Data validation and schema updates
    EXEC LogDeploymentStep 'Schema Preparation', 'Starting', 'Preparing database schema';
    
    -- Ensure AgencyCode column is large enough
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'AgencyMasterLayer2' 
        AND COLUMN_NAME = 'AgencyCode' 
        AND CHARACTER_MAXIMUM_LENGTH >= 100
    )
    BEGIN
        ALTER TABLE AgencyMasterLayer2 ALTER COLUMN AgencyCode NVARCHAR(100) NOT NULL;
        EXEC LogDeploymentStep 'Schema Preparation', 'Info', 'AgencyCode column expanded to NVARCHAR(100)';
    END
    
    EXEC LogDeploymentStep 'Schema Preparation', 'Completed', 'Schema prepared successfully';
    
    -- Step 1: Add missing agencies with NTEXT handling
    EXEC LogDeploymentStep 'Agency Mapping', 'Starting', 'Adding agencies with NTEXT handling';
    
    INSERT INTO AgencyMasterLayer2 (AgencyCode, AgencyName, IsActive, DataQualityScore, CreatedDate)
    SELECT DISTINCT 
        CASE 
            WHEN g.AgencyCode IS NOT NULL THEN 
                SUBSTRING(LTRIM(RTRIM(CAST(g.AgencyCode AS NVARCHAR(MAX)))), 1, 100)
            ELSE 'UNKNOWN'
        END as AgencyCode,
        CASE 
            WHEN g.AgencyName IS NOT NULL THEN 
                SUBSTRING(LTRIM(RTRIM(CAST(g.AgencyName AS NVARCHAR(MAX)))), 1, 500)
            ELSE 'Unknown Agency'
        END as AgencyName,
        1, -- IsActive
        80, -- DataQualityScore
        GETDATE()
    FROM Grants g
    WHERE g.AgencyCode IS NOT NULL 
    AND g.AgencyName IS NOT NULL
    AND CAST(g.AgencyCode AS NVARCHAR(MAX)) != ''
    AND CAST(g.AgencyName AS NVARCHAR(MAX)) != ''
    AND NOT EXISTS (
        SELECT 1 FROM AgencyMasterLayer2 a 
        WHERE a.AgencyCode = SUBSTRING(LTRIM(RTRIM(CAST(g.AgencyCode AS NVARCHAR(MAX)))), 1, 100)
    );
    
    DECLARE @NewAgenciesCount INT = @@ROWCOUNT;
    DECLARE @AgencyMessage NVARCHAR(100);
    SET @AgencyMessage = 'Added ' + CAST(@NewAgenciesCount AS NVARCHAR(10)) + ' new agencies';
    EXEC LogDeploymentStep 'Agency Mapping', 'Completed', @AgencyMessage;
    
    -- Step 2: Add categories with NTEXT handling
    EXEC LogDeploymentStep 'Category Mapping', 'Starting', 'Adding categories with NTEXT handling';
    
    INSERT INTO CategoryMasterLayer2 (CategoryName, CategoryGroup, IsActive, CreatedDate)
    SELECT DISTINCT 
        SUBSTRING(LTRIM(RTRIM(CAST(g.Category AS NVARCHAR(MAX)))), 1, 255) as CategoryName,
        CASE 
            WHEN CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Science%' OR CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Research%' OR CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Technology%' THEN 'STEM'
            WHEN CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Education%' OR CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Training%' OR CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Academic%' THEN 'Education and Training'
            WHEN CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Health%' OR CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Medical%' OR CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Clinical%' THEN 'Medical and Health Sciences'
            WHEN CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Environment%' OR CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Conservation%' OR CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Climate%' THEN 'Environmental Sciences'
            WHEN CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Agriculture%' OR CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Food%' OR CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Rural%' THEN 'Food and Agriculture'
            WHEN CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Energy%' OR CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Power%' OR CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Renewable%' THEN 'Energy and Environment'
            WHEN CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Transportation%' OR CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Infrastructure%' THEN 'Transportation and Infrastructure'
            WHEN CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Housing%' OR CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Community%' OR CAST(g.Category AS NVARCHAR(MAX)) LIKE '%Development%' THEN 'Housing and Community Development'
            ELSE 'General'
        END as CategoryGroup,
        1, -- IsActive
        GETDATE()
    FROM Grants g
    WHERE g.Category IS NOT NULL 
    AND CAST(g.Category AS NVARCHAR(MAX)) != ''
    AND NOT EXISTS (
        SELECT 1 FROM CategoryMasterLayer2 c 
        WHERE c.CategoryName = SUBSTRING(LTRIM(RTRIM(CAST(g.Category AS NVARCHAR(MAX)))), 1, 255)
    );
    
    DECLARE @NewCategoriesCount INT = @@ROWCOUNT;
    DECLARE @CategoryMessage NVARCHAR(100);
    SET @CategoryMessage = 'Added ' + CAST(@NewCategoriesCount AS NVARCHAR(10)) + ' new categories';
    EXEC LogDeploymentStep 'Category Mapping', 'Completed', @CategoryMessage;
    
    -- Step 3: Migrate grants data with full NTEXT handling
    EXEC LogDeploymentStep 'Grants Migration', 'Starting', 'Migrating grants with NTEXT handling';
    
    INSERT INTO CleanedGrantsLayer2 (
        OpportunityID,
        Title,
        Description,
        AwardCeiling,
        AwardFloor,
        EstimatedTotalFunding,
        ExpectedAwards,
        CFDANumbers,
        PostedDate,
        CloseDate,
        Status,
        EligibilityFullText,
        Keywords,
        AdditionalInfoURL,
        ContactEmail,
        AgencyID,
        CategoryID,
        IsActive,
        DataQualityScore,
        CreatedDate
    )
    SELECT 
        COALESCE(
            NULLIF(LTRIM(RTRIM(CAST(g.RowKey AS NVARCHAR(255)))), ''), 
            CAST(g.ID AS NVARCHAR(255))
        ) as OpportunityID,
        CASE 
            WHEN g.Title IS NOT NULL THEN CAST(g.Title AS NVARCHAR(MAX))
            ELSE 'Untitled Grant'
        END as Title,
        CASE 
            WHEN g.Description IS NOT NULL THEN CAST(g.Description AS NVARCHAR(MAX))
            ELSE NULL
        END as Description,
        -- Safe numeric conversion
        CASE 
            WHEN g.AwardCeiling IS NOT NULL 
            AND TRY_CAST(g.AwardCeiling AS DECIMAL(18,2)) IS NOT NULL 
            AND TRY_CAST(g.AwardCeiling AS DECIMAL(18,2)) > 0 
            THEN TRY_CAST(g.AwardCeiling AS DECIMAL(18,2))
            ELSE NULL 
        END,
        CASE 
            WHEN g.AwardFloor IS NOT NULL 
            AND TRY_CAST(g.AwardFloor AS DECIMAL(18,2)) IS NOT NULL 
            AND TRY_CAST(g.AwardFloor AS DECIMAL(18,2)) > 0 
            THEN TRY_CAST(g.AwardFloor AS DECIMAL(18,2))
            ELSE NULL 
        END,
        CASE 
            WHEN g.EstimatedTotalFunding IS NOT NULL 
            AND TRY_CAST(g.EstimatedTotalFunding AS DECIMAL(18,2)) IS NOT NULL 
            AND TRY_CAST(g.EstimatedTotalFunding AS DECIMAL(18,2)) > 0 
            THEN TRY_CAST(g.EstimatedTotalFunding AS DECIMAL(18,2))
            ELSE NULL 
        END,
        CASE 
            WHEN g.ExpectedAwards IS NOT NULL 
            AND TRY_CAST(g.ExpectedAwards AS INT) IS NOT NULL 
            AND TRY_CAST(g.ExpectedAwards AS INT) > 0 
            THEN TRY_CAST(g.ExpectedAwards AS INT)
            ELSE NULL 
        END,
        CASE 
            WHEN g.CFDANumbers IS NOT NULL THEN CAST(g.CFDANumbers AS NVARCHAR(500))
            ELSE NULL
        END,
        TRY_CAST(g.PostedDate AS DATETIME),
        TRY_CAST(g.CloseDate AS DATETIME),
        CASE 
            WHEN g.CloseDate IS NOT NULL AND TRY_CAST(g.CloseDate AS DATETIME) < GETDATE() THEN 'Closed'
            WHEN g.CloseDate IS NOT NULL AND TRY_CAST(g.CloseDate AS DATETIME) >= GETDATE() THEN 'Active'
            ELSE 'Active'
        END as Status,
        CASE 
            WHEN g.EligibleApplicants IS NOT NULL THEN CAST(g.EligibleApplicants AS NVARCHAR(MAX))
            ELSE NULL
        END,
        -- Safe keyword concatenation
        CASE 
            WHEN g.Category IS NOT NULL AND g.FundingType IS NOT NULL 
            THEN CAST(g.Category AS NVARCHAR(MAX)) + '; ' + CAST(g.FundingType AS NVARCHAR(MAX))
            WHEN g.Category IS NOT NULL 
            THEN CAST(g.Category AS NVARCHAR(MAX))
            WHEN g.FundingType IS NOT NULL 
            THEN CAST(g.FundingType AS NVARCHAR(MAX))
            ELSE NULL
        END as Keywords,
        CASE 
            WHEN g.AdditionalInfoURL IS NOT NULL THEN CAST(g.AdditionalInfoURL AS NVARCHAR(1000))
            ELSE NULL
        END,
        CASE 
            WHEN g.GrantorEmail IS NOT NULL THEN CAST(g.GrantorEmail AS NVARCHAR(255))
            ELSE NULL
        END,
        -- Safe agency mapping
        COALESCE(
            (SELECT TOP 1 AgencyID FROM AgencyMasterLayer2 a 
             WHERE a.AgencyCode = SUBSTRING(LTRIM(RTRIM(CAST(g.AgencyCode AS NVARCHAR(MAX)))), 1, 100)),
            (SELECT TOP 1 AgencyID FROM AgencyMasterLayer2 a 
             WHERE a.AgencyName = SUBSTRING(LTRIM(RTRIM(CAST(g.AgencyName AS NVARCHAR(MAX)))), 1, 500)),
            (SELECT TOP 1 AgencyID FROM AgencyMasterLayer2)
        ),
        -- Safe category mapping
        COALESCE(
            (SELECT TOP 1 CategoryID FROM CategoryMasterLayer2 c 
             WHERE c.CategoryName = SUBSTRING(LTRIM(RTRIM(CAST(g.Category AS NVARCHAR(MAX)))), 1, 255)),
            (SELECT TOP 1 CategoryID FROM CategoryMasterLayer2 WHERE CategoryName = 'Science and Technology')
        ),
        1, -- IsActive
        CASE 
            WHEN g.Title IS NOT NULL AND g.Description IS NOT NULL AND g.AgencyCode IS NOT NULL THEN 90
            WHEN g.Title IS NOT NULL AND g.AgencyCode IS NOT NULL THEN 80
            ELSE 70
        END as DataQualityScore,
        GETDATE()
    FROM Grants g
    WHERE g.Title IS NOT NULL
    AND CAST(g.Title AS NVARCHAR(MAX)) != ''
    AND NOT EXISTS (
        SELECT 1 FROM CleanedGrantsLayer2 c2 
        WHERE c2.OpportunityID = COALESCE(
            NULLIF(LTRIM(RTRIM(CAST(g.RowKey AS NVARCHAR(255)))), ''), 
            CAST(g.ID AS NVARCHAR(255))
        )
    );
    
    DECLARE @MigratedCount INT = @@ROWCOUNT;
    DECLARE @MigratedMessage NVARCHAR(100);
    SET @MigratedMessage = 'Migrated ' + CAST(@MigratedCount AS NVARCHAR(10)) + ' records to CleanedGrantsLayer2';
    EXEC LogDeploymentStep 'Grants Migration', 'Completed', @MigratedMessage;
    
    -- Step 4: Create eligibility mappings with NTEXT handling
    EXEC LogDeploymentStep 'Eligibility Mapping', 'Starting', 'Creating eligibility mappings with NTEXT handling';
    
    INSERT INTO GrantEligibilityLayer2 (GrantID, EligibilityID)
    SELECT DISTINCT
        c.GrantID,
        e.EligibilityID
    FROM CleanedGrantsLayer2 c
    CROSS JOIN EligibilityMasterLayer2 e
    WHERE c.EligibilityFullText IS NOT NULL
    AND c.EligibilityFullText != ''
    AND (
        (e.EligibilityType = 'Public/State Institutions' AND (
            c.EligibilityFullText LIKE '%Public%' OR 
            c.EligibilityFullText LIKE '%State%' OR 
            c.EligibilityFullText LIKE '%institution%higher%education%' OR
            c.EligibilityFullText LIKE '%university%' OR
            c.EligibilityFullText LIKE '%college%'
        )) OR
        (e.EligibilityType = 'Private Institutions' AND (
            c.EligibilityFullText LIKE '%Private%' OR 
            c.EligibilityFullText LIKE '%private%institution%'
        )) OR
        (e.EligibilityType = 'Nonprofit Organizations' AND (
            c.EligibilityFullText LIKE '%nonprofit%' OR 
            c.EligibilityFullText LIKE '%501%c%3%' OR
            c.EligibilityFullText LIKE '%non-profit%'
        )) OR
        (e.EligibilityType = 'For-profit Organizations' AND (
            c.EligibilityFullText LIKE '%profit%' OR 
            c.EligibilityFullText LIKE '%business%' OR
            c.EligibilityFullText LIKE '%commercial%'
        )) OR
        (e.EligibilityType = 'State Governments' AND (
            c.EligibilityFullText LIKE '%state%government%' OR 
            c.EligibilityFullText LIKE '%state%agencie%'
        )) OR
        (e.EligibilityType = 'Local Governments' AND (
            c.EligibilityFullText LIKE '%local%government%' OR 
            c.EligibilityFullText LIKE '%city%' OR
            c.EligibilityFullText LIKE '%county%' OR
            c.EligibilityFullText LIKE '%municipal%'
        )) OR
        (e.EligibilityType = 'Tribal Governments' AND (
            c.EligibilityFullText LIKE '%tribal%' OR 
            c.EligibilityFullText LIKE '%tribe%' OR
            c.EligibilityFullText LIKE '%native%american%'
        )) OR
        (e.EligibilityType = 'Individual Researchers' AND (
            c.EligibilityFullText LIKE '%individual%' OR 
            c.EligibilityFullText LIKE '%researcher%' OR
            c.EligibilityFullText LIKE '%scholar%'
        ))
    )
    AND NOT EXISTS (
        SELECT 1 FROM GrantEligibilityLayer2 ge 
        WHERE ge.GrantID = c.GrantID AND ge.EligibilityID = e.EligibilityID
    );
    
    DECLARE @EligibilityMappingsCount INT = @@ROWCOUNT;
    DECLARE @EligibilityMessage NVARCHAR(100);
    SET @EligibilityMessage = 'Created ' + CAST(@EligibilityMappingsCount AS NVARCHAR(10)) + ' eligibility mappings';
    EXEC LogDeploymentStep 'Eligibility Mapping', 'Completed', @EligibilityMessage;
    
    -- Step 5: Update agency statistics
    EXEC LogDeploymentStep 'Agency Stats Update', 'Starting', 'Updating agency statistics';
    
    MERGE AgencyStatsLayer3 AS target
    USING (
        SELECT 
            a.AgencyID,
            COUNT(g.GrantID) as TotalGrants,
            COUNT(CASE WHEN g.Status = 'Active' THEN 1 END) as ActiveGrants,
            COUNT(CASE WHEN g.Status = 'Closed' THEN 1 END) as ClosedGrants,
            SUM(COALESCE(g.EstimatedTotalFunding, 0)) as TotalFunding,
            CASE WHEN COUNT(g.GrantID) > 0 THEN AVG(COALESCE(g.EstimatedTotalFunding, 0)) ELSE 0 END as AvgFunding,
            COUNT(DISTINCT g.CategoryID) as CategoryCount,
            AVG(CASE WHEN g.PostedDate IS NOT NULL AND g.CloseDate IS NOT NULL 
                THEN DATEDIFF(DAY, g.PostedDate, g.CloseDate) END) as AvgApplicationPeriod
        FROM AgencyMasterLayer2 a
        LEFT JOIN CleanedGrantsLayer2 g ON a.AgencyID = g.AgencyID
        GROUP BY a.AgencyID
    ) AS source ON target.AgencyID = source.AgencyID
    WHEN MATCHED THEN
        UPDATE SET 
            TotalGrants = source.TotalGrants,
            ActiveGrants = source.ActiveGrants,
            ClosedGrants = source.ClosedGrants,
            TotalFunding = source.TotalFunding,
            AvgFunding = source.AvgFunding,
            CategoryCount = source.CategoryCount,
            AvgApplicationPeriod = source.AvgApplicationPeriod,
            LastCalculated = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (AgencyID, TotalGrants, ActiveGrants, ClosedGrants, TotalFunding, AvgFunding, CategoryCount, AvgApplicationPeriod, LastCalculated)
        VALUES (source.AgencyID, source.TotalGrants, source.ActiveGrants, source.ClosedGrants, 
               source.TotalFunding, source.AvgFunding, source.CategoryCount, source.AvgApplicationPeriod, GETDATE());
    
    EXEC LogDeploymentStep 'Agency Stats Update', 'Completed', 'Agency statistics updated successfully';
    
    -- Step 6: Create business insights
    EXEC LogDeploymentStep 'Business Insights', 'Starting', 'Creating business insights';
    
    INSERT INTO GrantBusinessViewLayer3 (GrantID, CompetitionLevel, FundingTier, RecommendedFocus, CreatedDate)
    SELECT 
        g.GrantID,
        CASE 
            WHEN g.EstimatedTotalFunding >= 10000000 THEN 'Very High'
            WHEN g.EstimatedTotalFunding >= 1000000 THEN 'High'
            WHEN g.EstimatedTotalFunding >= 100000 THEN 'Medium'
            WHEN g.EstimatedTotalFunding >= 10000 THEN 'Low'
            ELSE 'Very Low'
        END as CompetitionLevel,
        CASE 
            WHEN g.EstimatedTotalFunding >= 5000000 THEN 'Tier 1 - Major Program'
            WHEN g.EstimatedTotalFunding >= 1000000 THEN 'Tier 2 - Significant Grant'
            WHEN g.EstimatedTotalFunding >= 100000 THEN 'Tier 3 - Standard Grant'
            WHEN g.EstimatedTotalFunding >= 10000 THEN 'Tier 4 - Small Grant'
            ELSE 'Tier 5 - Micro Grant'
        END as FundingTier,
        CASE 
            WHEN g.DaysUntilDeadline <= 7 AND g.DaysUntilDeadline > 0 THEN 'URGENT: Application due within 1 week'
            WHEN g.DaysUntilDeadline <= 30 AND g.DaysUntilDeadline > 7 THEN 'HIGH PRIORITY: Application due within 30 days'
            WHEN g.DaysUntilDeadline <= 60 AND g.DaysUntilDeadline > 30 THEN 'MEDIUM PRIORITY: Application due within 60 days'
            WHEN g.DaysUntilDeadline > 60 THEN 'PLAN AHEAD: Good opportunity for future planning'
            WHEN g.DaysUntilDeadline <= 0 THEN 'CLOSED: Review for future similar opportunities'
            ELSE 'OPEN: No specific deadline information'
        END as RecommendedFocus,
        GETDATE()
    FROM CleanedGrantsLayer2 g
    WHERE NOT EXISTS (
        SELECT 1 FROM GrantBusinessViewLayer3 gbv WHERE gbv.GrantID = g.GrantID
    );
    
    DECLARE @BusinessInsightsCount INT = @@ROWCOUNT;
    DECLARE @BusinessMessage NVARCHAR(100);
    SET @BusinessMessage = 'Created ' + CAST(@BusinessInsightsCount AS NVARCHAR(10)) + ' business insights';
    EXEC LogDeploymentStep 'Business Insights', 'Completed', @BusinessMessage;
    
    -- Final summary
    DECLARE @FinalGrantsCount INT, @FinalAgenciesCount INT, @FinalCategoriesCount INT, @FinalEligibilityCount INT;
    SELECT @FinalGrantsCount = COUNT(*) FROM CleanedGrantsLayer2;
    SELECT @FinalAgenciesCount = COUNT(*) FROM AgencyMasterLayer2;
    SELECT @FinalCategoriesCount = COUNT(*) FROM CategoryMasterLayer2;
    SELECT @FinalEligibilityCount = COUNT(*) FROM GrantEligibilityLayer2;
    
    DECLARE @SummaryMessage NVARCHAR(500);
    SET @SummaryMessage = 'Bulletproof migration completed successfully. Grants: ' + 
                         CAST(@FinalGrantsCount AS NVARCHAR(10)) + ', Agencies: ' + 
                         CAST(@FinalAgenciesCount AS NVARCHAR(10)) + ', Categories: ' + 
                         CAST(@FinalCategoriesCount AS NVARCHAR(10)) + ', Eligibility Mappings: ' +
                         CAST(@FinalEligibilityCount AS NVARCHAR(10));
    
    EXEC LogDeploymentStep 'Data Migration', 'Summary', @SummaryMessage;

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorProcedure NVARCHAR(200) = ISNULL(ERROR_PROCEDURE(), 'migrate_existing_data_bulletproof.sql');
    
    DECLARE @FullErrorMessage NVARCHAR(4000);
    SET @FullErrorMessage = 'Bulletproof Migration Error in ' + @ErrorProcedure + 
                           ' at Line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
                           ': ' + @ErrorMessage;
    
    EXEC LogDeploymentStep 'Data Migration', 'Failed', @FullErrorMessage;
    RAISERROR(@FullErrorMessage, 16, 1);
END CATCH;

PRINT '===============================================';
PRINT 'Bulletproof data migration completed at ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '===============================================';
GO