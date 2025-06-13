-- ===================================
-- GRANTS.GOV API AZURE - MONITORING SETUP
-- ===================================

USE GrantsGovDB;
GO

-- Create Monitoring Schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'monitoring')
BEGIN
    EXEC('CREATE SCHEMA monitoring');
END
GO

-- Create Application Log Table
CREATE TABLE IF NOT EXISTS monitoring.ApplicationLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    LogLevel VARCHAR(10) NOT NULL,
    LogComponent VARCHAR(50) NOT NULL,
    LogMessage NVARCHAR(MAX) NOT NULL,
    LogDate DATETIME NOT NULL DEFAULT GETDATE(),
    Username VARCHAR(50),
    SessionID VARCHAR(100),
    ExceptionDetails NVARCHAR(MAX),
    ClientIP VARCHAR(50)
);
GO

-- Create API Request Log Table
CREATE TABLE IF NOT EXISTS monitoring.APIRequestLog (
    RequestID INT IDENTITY(1,1) PRIMARY KEY,
    Endpoint VARCHAR(100) NOT NULL,
    Method VARCHAR(10) NOT NULL,
    StatusCode INT NOT NULL,
    RequestTime DATETIME NOT NULL DEFAULT GETDATE(),
    ResponseTime INT NOT NULL, -- in milliseconds
    ClientIP VARCHAR(50),
    UserAgent VARCHAR(255),
    QueryParams NVARCHAR(MAX),
    AuthUser VARCHAR(50),
    ErrorDetails NVARCHAR(MAX),
    RequestSize INT,
    ResponseSize INT
);
GO

-- Create Maintenance Log Table
CREATE TABLE IF NOT EXISTS monitoring.MaintenanceLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    OperationType VARCHAR(50) NOT NULL,
    OperationDetails NVARCHAR(MAX) NOT NULL,
    RowsAffected INT,
    OperationDate DATETIME NOT NULL DEFAULT GETDATE(),
    ExecutedBy VARCHAR(50) DEFAULT SYSTEM_USER,
    ExecutionTime INT, -- in milliseconds
    Success BIT NOT NULL DEFAULT 1
);
GO

-- Create Health Check Log Table
CREATE TABLE IF NOT EXISTS monitoring.HealthCheckLog (
    CheckID INT IDENTITY(1,1) PRIMARY KEY,
    CheckDate DATETIME NOT NULL DEFAULT GETDATE(),
    CheckType VARCHAR(100) NOT NULL,
    CheckDetails NVARCHAR(MAX),
    CheckStatus VARCHAR(10) NOT NULL, -- OK, Warning, Error
    ActionTaken NVARCHAR(MAX),
    ResolvedDate DATETIME
);
GO

-- Create Performance Metrics Table
CREATE TABLE IF NOT EXISTS monitoring.PerformanceMetrics (
    MetricID INT IDENTITY(1,1) PRIMARY KEY,
    MetricDate DATETIME NOT NULL DEFAULT GETDATE(),
    MetricType VARCHAR(50) NOT NULL,
    MetricName VARCHAR(100) NOT NULL,
    MetricValue FLOAT NOT NULL,
    MetricUnit VARCHAR(20) NOT NULL,
    SamplePeriodSeconds INT NOT NULL
);
GO

-- Create Database Growth Tracking Table
CREATE TABLE IF NOT EXISTS monitoring.DatabaseGrowth (
    TrackingID INT IDENTITY(1,1) PRIMARY KEY,
    TrackingDate DATETIME NOT NULL DEFAULT GETDATE(),
    DatabaseSizeKB BIGINT NOT NULL,
    LogSizeKB BIGINT NOT NULL,
    TotalRowCount BIGINT,
    ActiveGrantsCount INT,
    ArchivedGrantsCount INT,
    TableCounts NVARCHAR(MAX) -- JSON with counts for each table
);
GO

-- Create Monitoring Views
CREATE OR ALTER VIEW monitoring.vw_PerformanceOverview AS
SELECT
    CONVERT(DATE, MetricDate) AS MetricDay,
    MetricType,
    MetricName,
    AVG(MetricValue) AS AvgValue,
    MAX(MetricValue) AS MaxValue,
    MIN(MetricValue) AS MinValue,
    STDEV(MetricValue) AS StdDevValue,
    COUNT(*) AS SampleCount
FROM 
    monitoring.PerformanceMetrics
GROUP BY
    CONVERT(DATE, MetricDate),
    MetricType,
    MetricName;
GO

CREATE OR ALTER VIEW monitoring.vw_APIRequestSummary AS
SELECT
    CONVERT(DATE, RequestTime) AS RequestDate,
    Endpoint,
    COUNT(*) AS RequestCount,
    AVG(ResponseTime) AS AvgResponseTime,
    MAX(ResponseTime) AS MaxResponseTime,
    MIN(ResponseTime) AS MinResponseTime,
    SUM(CASE WHEN StatusCode >= 400 THEN 1 ELSE 0 END) AS ErrorCount,
    SUM(CASE WHEN StatusCode >= 400 THEN 0 ELSE 1 END) AS SuccessCount,
    SUM(RequestSize) AS TotalRequestSize,
    SUM(ResponseSize) AS TotalResponseSize
FROM
    monitoring.APIRequestLog
GROUP BY
    CONVERT(DATE, RequestTime),
    Endpoint;
GO

CREATE OR ALTER VIEW monitoring.vw_ApplicationErrorSummary AS
SELECT
    CONVERT(DATE, LogDate) AS LogDate,
    LogComponent,
    LogLevel,
    COUNT(*) AS ErrorCount,
    MIN(LogDate) AS FirstOccurrence,
    MAX(LogDate) AS LastOccurrence
FROM
    monitoring.ApplicationLog
WHERE
    LogLevel IN ('ERROR', 'CRITICAL')
GROUP BY
    CONVERT(DATE, LogDate),
    LogComponent,
    LogLevel;
GO

CREATE OR ALTER VIEW monitoring.vw_DatabaseGrowthTrend AS
SELECT
    TrackingDate,
    DatabaseSizeKB,
    LogSizeKB,
    TotalRowCount,
    ActiveGrantsCount,
    ArchivedGrantsCount,
    LAG(DatabaseSizeKB) OVER (ORDER BY TrackingDate) AS PreviousSize,
    (DatabaseSizeKB - LAG(DatabaseSizeKB) OVER (ORDER BY TrackingDate)) AS SizeChange,
    CASE 
        WHEN LAG(DatabaseSizeKB) OVER (ORDER BY TrackingDate) = 0 THEN 0
        ELSE (DatabaseSizeKB - LAG(DatabaseSizeKB) OVER (ORDER BY TrackingDate)) * 100.0 / LAG(DatabaseSizeKB) OVER (ORDER BY TrackingDate)
    END AS GrowthPercent
FROM
    monitoring.DatabaseGrowth;
GO

-- Create Stored Procedures for Monitoring
CREATE OR ALTER PROCEDURE monitoring.CollectDatabaseMetrics
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @TableCounts NVARCHAR(MAX) = N'{';
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @TableName NVARCHAR(128);
    DECLARE @RowCount BIGINT;
    DECLARE @TotalRows BIGINT = 0;
    DECLARE @ActiveGrants INT;
    DECLARE @ArchivedGrants INT;
    
    -- Get count of active grants
    SELECT @ActiveGrants = COUNT(*) FROM CleanedGrantsLayer2 WHERE IsActive = 1;
    
    -- Get count of archived grants
    SELECT @ArchivedGrants = COUNT(*) FROM ArchivedGrantsLayer2;
    
    -- Create temporary table to store table counts
    CREATE TABLE #TableCounts (
        TableName NVARCHAR(128),
        RowCount BIGINT
    );
    
    -- Get row counts for all user tables
    INSERT INTO #TableCounts
    SELECT 
        t.name AS TableName,
        SUM(p.[rows]) AS RowCount
    FROM 
        sys.tables t
    INNER JOIN
        sys.partitions p ON t.object_id = p.object_id
    WHERE 
        p.index_id IN (0, 1) -- heap or clustered index
        AND t.is_ms_shipped = 0 -- not a system table
    GROUP BY 
        t.name
    ORDER BY 
        SUM(p.[rows]) DESC;
    
    -- Calculate total rows
    SELECT @TotalRows = SUM(RowCount) FROM #TableCounts;
    
    -- Create JSON with table counts
    SELECT @TableCounts = @TableCounts + '"' + TableName + '":' + CAST(RowCount AS NVARCHAR(20)) + ','
    FROM #TableCounts;
    
    -- Remove trailing comma and close JSON
    SET @TableCounts = LEFT(@TableCounts, LEN(@TableCounts) - 1) + N'}';
    
    -- Get database and log file sizes
    DECLARE @DBSize BIGINT;
    DECLARE @LogSize BIGINT;
    
    SELECT 
        @DBSize = SUM(CASE WHEN type_desc = 'ROWS' THEN size END) * 8,
        @LogSize = SUM(CASE WHEN type_desc = 'LOG' THEN size END) * 8
    FROM 
        sys.database_files;
    
    -- Insert metrics into tracking table
    INSERT INTO monitoring.DatabaseGrowth (
        DatabaseSizeKB,
        LogSizeKB,
        TotalRowCount,
        ActiveGrantsCount,
        ArchivedGrantsCount,
        TableCounts
    )
    VALUES (
        @DBSize,
        @LogSize,
        @TotalRows,
        @ActiveGrants,
        @ArchivedGrants,
        @TableCounts
    );
    
    DROP TABLE #TableCounts;
    
    -- Return current metrics
    SELECT 
        @DBSize AS DatabaseSizeKB,
        @LogSize AS LogSizeKB,
        @TotalRows AS TotalRowCount,
        @ActiveGrants AS ActiveGrantsCount,
        @ArchivedGrants AS ArchivedGrantsCount;
END;
GO

-- Create Azure SQL specific monitoring view
CREATE OR ALTER VIEW monitoring.vw_AzureSQLResourceStats AS
SELECT TOP 100
    end_time,
    avg_cpu_percent,
    avg_data_io_percent,
    avg_log_write_percent,
    avg_memory_usage_percent,
    xtp_storage_percent,
    max_worker_percent,
    max_session_percent,
    dtu_consumption_percent
FROM
    sys.dm_db_resource_stats
ORDER BY
    end_time DESC;
GO