-- ===================================
-- GRANTS.GOV API AZURE - FUNCTIONS
-- ===================================

USE GrantsGovDB;
GO

-- Data Quality Assessment Function
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'FN' AND name = 'CalculateDataQuality')
    DROP FUNCTION CalculateDataQuality;
GO

CREATE FUNCTION CalculateDataQuality(@GrantID INT)
RETURNS DECIMAL(3,2)
AS
BEGIN
    DECLARE @Score DECIMAL(3,2) = 0;
    DECLARE @MaxScore DECIMAL(3,2) = 10;
    
    -- Check if grant exists
    IF NOT EXISTS (SELECT 1 FROM CleanedGrantsLayer2 WHERE GrantID = @GrantID)
        RETURN 0;
    
    -- Score based on completeness
    SELECT @Score = @Score +
        CASE WHEN Title IS NOT NULL AND LEN(Title) > 10 THEN 1.5 ELSE 0 END +
        CASE WHEN Description IS NOT NULL AND LEN(Description) > 50 THEN 1.5 ELSE 0 END +
        CASE WHEN AgencyID IS NOT NULL THEN 1.0 ELSE 0 END +
        CASE WHEN CategoryID IS NOT NULL THEN 1.0 ELSE 0 END +
        CASE WHEN PostedDate IS NOT NULL THEN 1.0 ELSE 0 END +
        CASE WHEN CloseDate IS NOT NULL THEN 1.0 ELSE 0 END +
        CASE WHEN AwardCeiling > 0 THEN 1.5 ELSE 0 END +
        CASE WHEN ContactEmail IS NOT NULL AND ContactEmail LIKE '%@%' THEN 1.0 ELSE 0 END +
        CASE WHEN CloseDate > PostedDate THEN 0.5 ELSE 0 END
    FROM CleanedGrantsLayer2
    WHERE GrantID = @GrantID;
    
    RETURN @Score;
END;
GO

-- Calculate Competition Level
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'FN' AND name = 'CalculateCompetitionLevel')
    DROP FUNCTION CalculateCompetitionLevel;
GO

CREATE FUNCTION CalculateCompetitionLevel(
    @AwardCeiling MONEY, 
    @ExpectedAwards INT,
    @HistoricalApplicants INT = NULL
)
RETURNS NVARCHAR(20)
AS
BEGIN
    DECLARE @Result NVARCHAR(20);
    
    -- Default competition level based on expected awards and funding amount
    IF @AwardCeiling >= 1000000 AND (@ExpectedAwards < 10 OR @ExpectedAwards IS NULL)
        SET @Result = 'High';
    ELSE IF @AwardCeiling >= 500000 AND @ExpectedAwards < 20
        SET @Result = 'Medium-High';
    ELSE IF @AwardCeiling >= 250000 OR @ExpectedAwards < 50
        SET @Result = 'Medium';
    ELSE
        SET @Result = 'Low';
        
    -- Adjust if we have historical applicant data
    IF @HistoricalApplicants IS NOT NULL
    BEGIN
        IF @HistoricalApplicants > 100 AND @ExpectedAwards < 20
            SET @Result = 'High';
        ELSE IF @HistoricalApplicants > 50 AND @ExpectedAwards < 10
            SET @Result = 'High';
    END
    
    RETURN @Result;
END;
GO

PRINT '✅ Functions created successfully';
GO
