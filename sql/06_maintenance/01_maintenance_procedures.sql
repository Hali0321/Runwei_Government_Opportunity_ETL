-- ===================================
-- GRANTS.GOV API AZURE - MAINTENANCE PROCEDURES
-- ===================================

USE GrantsGovDB;
GO

-- Index Maintenance Procedure
CREATE OR ALTER PROCEDURE maintenance.RebuildIndexes
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @TableName NVARCHAR(256);
    DECLARE @IndexName NVARCHAR(256);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @FragmentationLevel FLOAT;
    
    -- Create temp table to hold index info
    CREATE TABLE #IndexFragmentation (
        TableName NVARCHAR(256),
        IndexName NVARCHAR(256),
        FragmentationLevel FLOAT
    );
    
    -- Get fragmentation info for all indexes
    INSERT INTO #IndexFragmentation
    SELECT 
        OBJECT_NAME(ips.object_id) AS TableName,
        i.name AS IndexName,
        ips.avg_fragmentation_in_percent AS FragmentationLevel
    FROM 
        sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    INNER JOIN 
        sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE 
        ips.avg_fragmentation_in_percent > 5
        AND i.name IS NOT NULL;
    
    -- Process each index
    DECLARE IndexCursor CURSOR FOR
    SELECT TableName, IndexName, FragmentationLevel
    FROM #IndexFragmentation;
    
    OPEN IndexCursor;
    FETCH NEXT FROM IndexCursor INTO @TableName, @IndexName, @FragmentationLevel;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @FragmentationLevel >= 30
        BEGIN
            -- Rebuild indexes with high fragmentation
            SET @SQL = N'ALTER INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@TableName) + ' REBUILD WITH (ONLINE = ON)';
            BEGIN TRY
                EXEC sp_executesql @SQL;
                PRINT 'Rebuilt index ' + @IndexName + ' on table ' + @TableName;
            END TRY
            BEGIN CATCH
                -- If online rebuild fails, try offline
                SET @SQL = N'ALTER INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@TableName) + ' REBUILD';
                EXEC sp_executesql @SQL;
                PRINT 'Rebuilt index ' + @IndexName + ' on table ' + @TableName + ' (offline)';
            END CATCH
        END
        ELSE
        BEGIN
            -- Reorganize indexes with moderate fragmentation
            SET @SQL = N'ALTER INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@TableName) + ' REORGANIZE';
            EXEC sp_executesql @SQL;
            PRINT 'Reorganized index ' + @IndexName + ' on table ' + @TableName;
        END
        
        FETCH NEXT FROM IndexCursor INTO @TableName, @IndexName, @FragmentationLevel;
    END
    
    CLOSE IndexCursor;
    DEALLOCATE IndexCursor;
    DROP TABLE #IndexFragmentation;
    
    -- Update statistics after index maintenance
    EXEC sp_updatestats;
    PRINT 'Updated database statistics';
END;
GO

-- Archive Old Grants Procedure
CREATE OR ALTER PROCEDURE maintenance.ArchiveOldGrants
    @MonthsOld INT = 12
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @CutoffDate DATETIME = DATEADD(MONTH, -@MonthsOld, GETDATE());
    
    -- Archive grants that closed before the cutoff date
    INSERT INTO ArchivedGrantsLayer2 (
        GrantID, OpportunityID, Title, AgencyID, CategoryID, Description,
        AwardCeiling, AwardFloor, EstimatedTotalFunding, ExpectedAwards,
        CFDANumbers, PostedDate, CloseDate, Status, EligibilityFullText,
        Keywords, GrantsGovURL, AdditionalInfoURL, ContactEmail, ContactPhone,
        IsActive, ArchiveDate, ArchiveReason, DataQualityScore
    )
    SELECT 
        g.GrantID, g.OpportunityID, g.Title, g.AgencyID, g.CategoryID, g.Description,
        g.AwardCeiling, g.AwardFloor, g.EstimatedTotalFunding, g.ExpectedAwards,
        g.CFDANumbers, g.PostedDate, g.CloseDate, g.Status, g.EligibilityFullText,
        g.Keywords, g.GrantsGovURL, g.AdditionalInfoURL, g.ContactEmail, g.ContactPhone,
        0, GETDATE(), 'Automatic archive due to age', g.DataQualityScore
    FROM 
        CleanedGrantsLayer2 g
    WHERE 
        g.CloseDate < @CutoffDate
        AND g.IsActive = 1;
    
    -- Update related tables to mark these grants as inactive
    UPDATE CleanedGrantsLayer2
    SET IsActive = 0
    WHERE CloseDate < @CutoffDate
    AND IsActive = 1;
    
    -- Log the archive operation
    INSERT INTO MaintenanceLog (
        OperationType, 
        OperationDetails, 
        RowsAffected, 
        OperationDate
    )
    VALUES (
        'Archive', 
        'Archived grants older than ' + CAST(@MonthsOld AS VARCHAR) + ' months', 
        @@ROWCOUNT, 
        GETDATE()
    );
END;
GO

-- Database Health Check Procedure
CREATE OR ALTER PROCEDURE maintenance.DatabaseHealthCheck
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Result TABLE (
        CheckName NVARCHAR(100),
        CheckResult NVARCHAR(MAX),
        Status NVARCHAR(10)
    );
    
    -- Check for missing indexes
    INSERT INTO @Result
    SELECT 
        'Missing Indexes', 
        'Found ' + CAST(COUNT(*) AS VARCHAR) + ' tables missing recommended indexes', 
        CASE WHEN COUNT(*) > 0 THEN 'Warning' ELSE 'OK' END
    FROM (
        SELECT 
            t.name AS TableName,
            'Missing index on columns: ' + 
            STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY c.column_id) AS MissingIndexColumns
        FROM 
            sys.tables t
        INNER JOIN 
            sys.columns c ON t.object_id = c.object_id
        LEFT JOIN 
            sys.index_columns ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE 
            ic.column_id IS NULL
            AND c.is_identity = 0
            AND c.is_computed = 0
            AND c.name IN ('GrantID', 'AgencyID', 'CategoryID', 'CloseDate', 'IsActive')
        GROUP BY 
            t.name
    ) AS MissingIndexes;
    
    -- Check for fragmented indexes
    INSERT INTO @Result
    SELECT 
        'Fragmented Indexes', 
        'Found ' + CAST(COUNT(*) AS VARCHAR) + ' indexes with fragmentation > 30%', 
        CASE WHEN COUNT(*) > 0 THEN 'Warning' ELSE 'OK' END
    FROM 
        sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    INNER JOIN 
        sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE 
        ips.avg_fragmentation_in_percent > 30;
    
    -- Check for outdated statistics
    INSERT INTO @Result
    SELECT 
        'Outdated Statistics', 
        'Found ' + CAST(COUNT(*) AS VARCHAR) + ' statistics objects that need updating', 
        CASE WHEN COUNT(*) > 0 THEN 'Warning' ELSE 'OK' END
    FROM 
        sys.stats AS s
    CROSS APPLY 
        sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
    WHERE 
        sp.modification_counter > 100 + (sp.rows * 0.1);
    
    -- Check for data consistency
    INSERT INTO @Result
    SELECT 
        'Data Consistency', 
        'Found ' + CAST(COUNT(*) AS VARCHAR) + ' grants with invalid foreign keys', 
        CASE WHEN COUNT(*) > 0 THEN 'Error' ELSE 'OK' END
    FROM CleanedGrantsLayer2 g
    LEFT JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
    WHERE g.AgencyID IS NOT NULL AND a.AgencyID IS NULL;
    
    -- Write results to health check log
    INSERT INTO HealthCheckLog (
        CheckDate,
        CheckType,
        CheckDetails,
        CheckStatus
    )
    SELECT 
        GETDATE(),
        CheckName,
        CheckResult,
        Status
    FROM @Result;
    
    -- Return results
    SELECT * FROM @Result;
END;
GO

-- Purge Old Logs Procedure
CREATE OR ALTER PROCEDURE maintenance.PurgeOldLogs
    @DaysToKeep INT = 90
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @CutoffDate DATETIME = DATEADD(DAY, -@DaysToKeep, GETDATE());
    DECLARE @RowsDeleted INT = 0;
    
    -- Delete old application logs
    DELETE FROM ApplicationLog
    WHERE LogDate < @CutoffDate;
    
    SET @RowsDeleted = @RowsDeleted + @@ROWCOUNT;
    
    -- Delete old maintenance logs
    DELETE FROM MaintenanceLog
    WHERE OperationDate < @CutoffDate;
    
    SET @RowsDeleted = @RowsDeleted + @@ROWCOUNT;
    
    -- Delete old API request logs
    DELETE FROM APIRequestLog
    WHERE RequestTime < @CutoffDate;
    
    SET @RowsDeleted = @RowsDeleted + @@ROWCOUNT;
    
    -- Log the purge operation
    INSERT INTO MaintenanceLog (
        OperationType, 
        OperationDetails, 
        RowsAffected, 
        OperationDate
    )
    VALUES (
        'Log Purge', 
        'Purged logs older than ' + CAST(@DaysToKeep AS VARCHAR) + ' days', 
        @RowsDeleted, 
        GETDATE()
    );
    
    -- Return summary
    SELECT 'Purged ' + CAST(@RowsDeleted AS VARCHAR) + ' log entries' AS Result;
END;
GO