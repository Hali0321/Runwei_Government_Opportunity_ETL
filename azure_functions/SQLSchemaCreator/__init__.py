import azure.functions as func
import json
import logging
import pyodbc
from typing import Dict
import os
from datetime import datetime

# SQL Server connection configuration
SQL_SERVER = "grants-gov-sql-server.database.windows.net"
SQL_DATABASE = "GrantsGovDB"
SQL_USERNAME = "grantsadmin"
SQL_PASSWORD = "GrantsAdmin123!"

def main(req: func.HttpRequest) -> func.HttpResponse:
    """Create complete three-layer database schema from your SQL file"""
    
    logging.info('SQL Schema Creator function started')
    
    try:
        operation = req.params.get('operation', 'create_all')
        format_type = req.params.get('format', 'json')
        
        if operation == 'create_all':
            result = create_complete_schema()
        elif operation == 'create_layer1':
            result = create_layer1_tables()
        elif operation == 'create_layer2':
            result = create_layer2_tables()
        elif operation == 'create_layer3':
            result = create_layer3_tables()
        elif operation == 'create_procedures':
            result = create_stored_procedures()
        elif operation == 'test_connection':
            result = test_sql_connection()
        else:
            result = {"error": "Invalid operation. Use: create_all, create_layer1, create_layer2, create_layer3, create_procedures, test_connection"}
        
        if format_type == 'html':
            return generate_html_response(result, operation)
        else:
            return func.HttpResponse(
                json.dumps(result, default=str, indent=2),
                mimetype="application/json"
            )
            
    except Exception as e:
        logging.error(f"Schema Creator error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Schema creation failed: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

def get_sql_connection():
    """Get SQL Server connection"""
    try:
        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USERNAME};"
            f"PWD={SQL_PASSWORD};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        
        connection = pyodbc.connect(connection_string)
        return connection
        
    except Exception as e:
        logging.error(f"SQL connection failed: {str(e)}")
        raise

def test_sql_connection() -> Dict:
    """Test connection to your SQL Server"""
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        cursor.execute("SELECT @@VERSION, DB_NAME(), GETDATE()")
        result = cursor.fetchone()
        
        connection.close()
        
        return {
            "status": "success",
            "connection_test": "PASSED ✅",
            "server": SQL_SERVER,
            "database": SQL_DATABASE,
            "sql_version": result[0][:100] if result else "Unknown",
            "current_time": result[2] if result else datetime.utcnow().isoformat(),
            "ready_for_schema": True
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "connection_test": "FAILED ❌",
            "error": str(e),
            "server": SQL_SERVER,
            "database": SQL_DATABASE
        }

def create_complete_schema() -> Dict:
    """Create the complete three-layer schema"""
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        created_objects = []
        
        # Create Layer 1 tables
        layer1_result = execute_layer1_sql(cursor)
        created_objects.extend(layer1_result)
        
        # Create Layer 2 tables
        layer2_result = execute_layer2_sql(cursor)
        created_objects.extend(layer2_result)
        
        # Create Layer 3 tables
        layer3_result = execute_layer3_sql(cursor)
        created_objects.extend(layer3_result)
        
        # Create indexes
        indexes_result = create_all_indexes(cursor)
        created_objects.extend(indexes_result)
        
        # Create stored procedures
        procedures_result = create_all_procedures(cursor)
        created_objects.extend(procedures_result)
        
        connection.commit()
        connection.close()
        
        return {
            "status": "success",
            "message": "Complete three-layer database schema created successfully",
            "server": SQL_SERVER,
            "database": SQL_DATABASE,
            "created_objects": created_objects,
            "total_objects": len(created_objects),
            "layers": ["Layer 1 - Raw Data", "Layer 2 - Cleaned & Normalized", "Layer 3 - Business Views & Analytics"],
            "schema_version": "1.0",
            "created_at": datetime.utcnow().isoformat(),
            "next_step": "Run data migration to populate tables"
        }
        
    except Exception as e:
        return {"error": f"Complete schema creation failed: {str(e)}"}

def execute_layer1_sql(cursor) -> list:
    """Execute Layer 1 table creation SQL"""
    created_objects = []
    
    # Raw Grants Data Table
    raw_grants_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='RawGrantsLayer1' AND xtype='U')
    CREATE TABLE RawGrantsLayer1 (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        OpportunityNumber NVARCHAR(100) NOT NULL,
        OpportunityTitle NVARCHAR(500),
        AgencyName NVARCHAR(200),
        AgencyCode NVARCHAR(50),
        FundingDescription NTEXT,
        PostedDate NVARCHAR(50),
        CloseDate NVARCHAR(50),
        AwardCeiling NVARCHAR(50),
        AwardFloor NVARCHAR(50),
        CategoryOfFundingActivity NVARCHAR(200),
        FundingInstrumentType NVARCHAR(100),
        EligibleApplicants NTEXT,
        LinkToAdditionalInformation NVARCHAR(500),
        AssistanceListings NVARCHAR(200),
        ExpectedNumberOfAwards NVARCHAR(50),
        EstimatedTotalFunding NVARCHAR(50),
        GrantorContactEmail NVARCHAR(200),
        DataQualityScore DECIMAL(3,2) DEFAULT 5.0,
        ImportDate DATETIME2 DEFAULT GETUTCDATE(),
        SourceFile NVARCHAR(200) DEFAULT 'Azure_Table_Migration'
    );
    """
    
    cursor.execute(raw_grants_sql)
    created_objects.append("RawGrantsLayer1")
    
    # Raw Agencies Data Table
    raw_agencies_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='RawAgenciesLayer1' AND xtype='U')
    CREATE TABLE RawAgenciesLayer1 (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        AgencyName NVARCHAR(200) NOT NULL,
        AgencyCode NVARCHAR(50),
        FirstSeenDate DATETIME2 DEFAULT GETUTCDATE(),
        GrantCount INT DEFAULT 0,
        IsActive BIT DEFAULT 1
    );
    """
    
    cursor.execute(raw_agencies_sql)
    created_objects.append("RawAgenciesLayer1")
    
    # Raw Categories Data Table
    raw_categories_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='RawCategoriesLayer1' AND xtype='U')
    CREATE TABLE RawCategoriesLayer1 (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        CategoryName NVARCHAR(200) NOT NULL,
        CategoryDescription NVARCHAR(500),
        FirstSeenDate DATETIME2 DEFAULT GETUTCDATE(),
        GrantCount INT DEFAULT 0,
        IsActive BIT DEFAULT 1
    );
    """
    
    cursor.execute(raw_categories_sql)
    created_objects.append("RawCategoriesLayer1")
    
    return created_objects

def execute_layer2_sql(cursor) -> list:
    """Execute Layer 2 table creation SQL"""
    created_objects = []
    
    # Agency Master Table
    agency_master_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='AgencyMasterLayer2' AND xtype='U')
    CREATE TABLE AgencyMasterLayer2 (
        AgencyID INT IDENTITY(1,1) PRIMARY KEY,
        AgencyName NVARCHAR(200) NOT NULL UNIQUE,
        AgencyCode NVARCHAR(50),
        AgencyType NVARCHAR(100) DEFAULT 'Federal',
        Department NVARCHAR(200),
        Website NVARCHAR(500),
        ContactEmail NVARCHAR(200),
        IsActive BIT DEFAULT 1,
        GrantCount INT DEFAULT 0,
        TotalFunding MONEY DEFAULT 0,
        CreatedDate DATETIME2 DEFAULT GETUTCDATE(),
        LastUpdated DATETIME2 DEFAULT GETUTCDATE()
    );
    """
    
    cursor.execute(agency_master_sql)
    created_objects.append("AgencyMasterLayer2")
    
    # Category Master Table with Hierarchy
    category_master_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='CategoryMasterLayer2' AND xtype='U')
    CREATE TABLE CategoryMasterLayer2 (
        CategoryID INT IDENTITY(1,1) PRIMARY KEY,
        CategoryName NVARCHAR(200) NOT NULL,
        CategoryCode NVARCHAR(50),
        CategoryGroup NVARCHAR(100),
        ParentCategoryID INT NULL,
        CategoryLevel INT DEFAULT 1,
        CategoryDescription NVARCHAR(500),
        IsActive BIT DEFAULT 1,
        GrantCount INT DEFAULT 0,
        AvgFunding MONEY DEFAULT 0,
        CreatedDate DATETIME2 DEFAULT GETUTCDATE()
    );
    """
    
    cursor.execute(category_master_sql)
    created_objects.append("CategoryMasterLayer2")
    
    # Add foreign key constraint after table creation
    try:
        fk_sql = """
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_CategoryMaster_Parent')
        ALTER TABLE CategoryMasterLayer2 
        ADD CONSTRAINT FK_CategoryMaster_Parent 
        FOREIGN KEY (ParentCategoryID) REFERENCES CategoryMasterLayer2(CategoryID);
        """
        cursor.execute(fk_sql)
        created_objects.append("FK_CategoryMaster_Parent")
    except Exception as e:
        logging.warning(f"Foreign key creation warning: {str(e)}")
    
    # Cleaned Grants Table with Foreign Keys
    cleaned_grants_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='CleanedGrantsLayer2' AND xtype='U')
    CREATE TABLE CleanedGrantsLayer2 (
        GrantID INT IDENTITY(1,1) PRIMARY KEY,
        OpportunityID NVARCHAR(100) NOT NULL UNIQUE,
        Title NVARCHAR(500) NOT NULL,
        AgencyID INT,
        CategoryID INT,
        Description NTEXT,
        ShortDescription NVARCHAR(500),
        PostedDate DATE,
        CloseDate DATE,
        AwardCeiling MONEY,
        AwardFloor MONEY,
        EstimatedTotalFunding MONEY,
        ExpectedAwards INT,
        InstrumentType NVARCHAR(100),
        EligibilityRequirements NTEXT,
        CFDANumbers NVARCHAR(200),
        ContactEmail NVARCHAR(200),
        AdditionalInfoURL NVARCHAR(500),
        
        -- Calculated fields
        DaysUntilDeadline AS (DATEDIFF(day, GETDATE(), CloseDate)),
        GrantsGovURL AS ('https://www.grants.gov/search-results-detail/' + OpportunityID),
        Status AS (
            CASE 
                WHEN CloseDate < GETDATE() THEN 'Closed'
                WHEN DATEDIFF(day, GETDATE(), CloseDate) <= 7 THEN 'Closing Soon'
                WHEN DATEDIFF(day, GETDATE(), CloseDate) <= 30 THEN 'Closing This Month'
                ELSE 'Open'
            END
        ),
        
        -- Data Quality and Status
        DataQualityScore DECIMAL(3,2) DEFAULT 5.0,
        ValidationStatus NVARCHAR(50) DEFAULT 'Pending',
        IsActive BIT DEFAULT 1,
        ProcessingNotes NVARCHAR(500),
        
        -- Audit Fields
        CreatedDate DATETIME2 DEFAULT GETUTCDATE(),
        LastUpdated DATETIME2 DEFAULT GETUTCDATE(),
        ProcessedBy NVARCHAR(100) DEFAULT 'Azure_Function',
        OriginalRowID INT
    );
    """
    
    cursor.execute(cleaned_grants_sql)
    created_objects.append("CleanedGrantsLayer2")
    
    # Add foreign key constraints
    try:
        fk_agency_sql = """
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_CleanedGrants_Agency')
        ALTER TABLE CleanedGrantsLayer2 
        ADD CONSTRAINT FK_CleanedGrants_Agency 
        FOREIGN KEY (AgencyID) REFERENCES AgencyMasterLayer2(AgencyID);
        """
        cursor.execute(fk_agency_sql)
        created_objects.append("FK_CleanedGrants_Agency")
        
        fk_category_sql = """
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_CleanedGrants_Category')
        ALTER TABLE CleanedGrantsLayer2 
        ADD CONSTRAINT FK_CleanedGrants_Category 
        FOREIGN KEY (CategoryID) REFERENCES CategoryMasterLayer2(CategoryID);
        """
        cursor.execute(fk_category_sql)
        created_objects.append("FK_CleanedGrants_Category")
    except Exception as e:
        logging.warning(f"Foreign key creation warning: {str(e)}")
    
    # Grant-Agency Relationships
    relationships_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='GrantAgencyRelationshipsLayer2' AND xtype='U')
    CREATE TABLE GrantAgencyRelationshipsLayer2 (
        RelationshipID INT IDENTITY(1,1) PRIMARY KEY,
        GrantID INT NOT NULL,
        AgencyID INT NOT NULL,
        RelationshipType NVARCHAR(50) DEFAULT 'Grantor',
        FundingAmount MONEY,
        RelationshipDate DATE,
        IsActive BIT DEFAULT 1,
        Notes NVARCHAR(500)
    );
    """
    
    cursor.execute(relationships_sql)
    created_objects.append("GrantAgencyRelationshipsLayer2")
    
    return created_objects

def execute_layer3_sql(cursor) -> list:
    """Execute Layer 3 table creation SQL"""
    created_objects = []
    
    # Business View Table
    business_view_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='GrantBusinessViewLayer3' AND xtype='U')
    CREATE TABLE GrantBusinessViewLayer3 (
        BusinessViewID INT IDENTITY(1,1) PRIMARY KEY,
        GrantID INT NOT NULL,
        OpportunityID NVARCHAR(100) NOT NULL,
        
        -- Grant Information
        Title NVARCHAR(500),
        Description NTEXT,
        ShortDescription NVARCHAR(500),
        PostedDate DATE,
        CloseDate DATE,
        DaysUntilDeadline AS (DATEDIFF(day, GETDATE(), CloseDate)),
        
        -- Financial Information
        AwardCeiling MONEY,
        AwardFloor MONEY,
        FundingRange AS (
            CASE 
                WHEN AwardFloor > 0 AND AwardCeiling > 0 
                THEN FORMAT(AwardFloor, 'C0') + ' - ' + FORMAT(AwardCeiling, 'C0')
                WHEN AwardCeiling > 0 
                THEN 'Up to ' + FORMAT(AwardCeiling, 'C0')
                ELSE 'Not Disclosed'
            END
        ),
        EstimatedTotalFunding MONEY,
        ExpectedAwards INT,
        
        -- Agency Information (Denormalized for Performance)
        AgencyID INT,
        AgencyName NVARCHAR(200),
        AgencyType NVARCHAR(100),
        Department NVARCHAR(200),
        AgencyContactEmail NVARCHAR(200),
        
        -- Category Information (Denormalized for Performance)
        CategoryID INT,
        CategoryName NVARCHAR(200),
        CategoryGroup NVARCHAR(100),
        ParentCategoryName NVARCHAR(200),
        
        -- Business Analytics
        FundingTier NVARCHAR(50), -- High, Medium, Low based on amount
        CompetitionLevel NVARCHAR(50), -- High, Medium, Low based on similar grants
        OpportunityScore DECIMAL(5,2), -- 0-100 calculated score
        UrgencyLevel NVARCHAR(50), -- Critical, High, Medium, Low based on deadline
        
        -- Status and Flags
        Status NVARCHAR(50), -- Open, Closing Soon, Closed, etc.
        IsHighValue BIT,
        IsMultiYear BIT,
        RequiresPartnership BIT,
        IsCompetitive BIT,
        
        -- Geographic and Eligibility
        EligibilityRequirements NTEXT,
        TargetCommunity NVARCHAR(200),
        GeographicScope NVARCHAR(100),
        
        -- URLs and Links
        GrantsGovURL NVARCHAR(500),
        AdditionalInfoURL NVARCHAR(500),
        AgencyWebsite NVARCHAR(500),
        
        -- Metadata
        DataQualityScore DECIMAL(3,2),
        LastAnalyzed DATETIME2 DEFAULT GETUTCDATE(),
        ViewCreatedDate DATETIME2 DEFAULT GETUTCDATE()
    );
    """
    
    cursor.execute(business_view_sql)
    created_objects.append("GrantBusinessViewLayer3")
    
    # Agency Analytics Table
    agency_stats_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='AgencyStatsLayer3' AND xtype='U')
    CREATE TABLE AgencyStatsLayer3 (
        StatsID INT IDENTITY(1,1) PRIMARY KEY,
        AgencyID INT NOT NULL,
        AgencyName NVARCHAR(200),
        
        -- Grant Statistics
        TotalGrants INT DEFAULT 0,
        ActiveGrants INT DEFAULT 0,
        ClosedGrants INT DEFAULT 0,
        
        -- Financial Statistics
        TotalFunding MONEY DEFAULT 0,
        AvgFunding MONEY DEFAULT 0,
        MaxFunding MONEY DEFAULT 0,
        MinFunding MONEY DEFAULT 0,
        
        -- Performance Metrics
        FundingPercentile DECIMAL(5,2), -- 0-100
        PopularityScore DECIMAL(5,2), -- Based on grant volume
        ResponseTime DECIMAL(5,2), -- Days between post and close
        
        -- Time-based Analysis
        GrantsThisYear INT DEFAULT 0,
        GrantsLastYear INT DEFAULT 0,
        GrowthRate DECIMAL(5,2),
        
        -- Categories
        PrimaryCategoryID INT,
        TopCategories NVARCHAR(500), -- JSON or comma-separated
        
        -- Metadata
        StatsGeneratedDate DATETIME2 DEFAULT GETUTCDATE(),
        LastUpdated DATETIME2 DEFAULT GETUTCDATE()
    );
    """
    
    cursor.execute(agency_stats_sql)
    created_objects.append("AgencyStatsLayer3")
    
    # Category Analytics Table
    category_stats_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='CategoryStatsLayer3' AND xtype='U')
    CREATE TABLE CategoryStatsLayer3 (
        StatsID INT IDENTITY(1,1) PRIMARY KEY,
        CategoryID INT NOT NULL,
        CategoryName NVARCHAR(200),
        
        -- Grant Statistics
        TotalGrants INT DEFAULT 0,
        ActiveGrants INT DEFAULT 0,
        AvgFunding MONEY DEFAULT 0,
        TotalFunding MONEY DEFAULT 0,
        
        -- Agency Analysis
        AgencyCount INT DEFAULT 0,
        TopAgencyID INT,
        TopAgencyName NVARCHAR(200),
        
        -- Competition Analysis
        CompetitionLevel NVARCHAR(50),
        AvgCompetitors DECIMAL(5,2),
        SuccessRate DECIMAL(5,2),
        
        -- Trend Analysis
        TrendDirection NVARCHAR(50), -- Increasing, Decreasing, Stable
        SeasonalPattern NVARCHAR(200),
        
        -- Metadata
        StatsGeneratedDate DATETIME2 DEFAULT GETUTCDATE(),
        LastUpdated DATETIME2 DEFAULT GETUTCDATE()
    );
    """
    
    cursor.execute(category_stats_sql)
    created_objects.append("CategoryStatsLayer3")
    
    # Funding Analytics Table for Time-Series Analysis
    funding_analytics_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='FundingAnalyticsLayer3' AND xtype='U')
    CREATE TABLE FundingAnalyticsLayer3 (
        AnalyticsID INT IDENTITY(1,1) PRIMARY KEY,
        AnalysisDate DATE NOT NULL,
        AnalysisType NVARCHAR(50) DEFAULT 'Daily',
        
        -- Overall Metrics
        TotalOpportunities INT DEFAULT 0,
        TotalFunding MONEY DEFAULT 0,
        AvgFunding MONEY DEFAULT 0,
        
        -- By Agency
        TopAgencyID INT,
        TopAgencyFunding MONEY DEFAULT 0,
        
        -- By Category
        TopCategoryID INT,
        TopCategoryFunding MONEY DEFAULT 0,
        
        -- Trends
        FundingGrowth DECIMAL(5,2),
        OpportunityGrowth DECIMAL(5,2),
        
        -- Geographic Analysis
        StateDistribution NVARCHAR(MAX), -- JSON format
        RegionalTrends NVARCHAR(MAX), -- JSON format
        
        CreatedDate DATETIME2 DEFAULT GETUTCDATE()
    );
    """
    
    cursor.execute(funding_analytics_sql)
    created_objects.append("FundingAnalyticsLayer3")
    
    return created_objects

def create_all_indexes(cursor) -> list:
    """Create all performance indexes"""
    created_objects = []
    
    indexes = [
        "CREATE INDEX IX_RawGrants_OpportunityNumber ON RawGrantsLayer1(OpportunityNumber);",
        "CREATE INDEX IX_RawGrants_AgencyName ON RawGrantsLayer1(AgencyName);",
        "CREATE INDEX IX_RawGrants_ImportDate ON RawGrantsLayer1(ImportDate);",
        "CREATE INDEX IX_CleanedGrants_OpportunityID ON CleanedGrantsLayer2(OpportunityID);",
        "CREATE INDEX IX_CleanedGrants_AgencyID ON CleanedGrantsLayer2(AgencyID);",
        "CREATE INDEX IX_CleanedGrants_CategoryID ON CleanedGrantsLayer2(CategoryID);",
        "CREATE INDEX IX_CleanedGrants_PostedDate ON CleanedGrantsLayer2(PostedDate);",
        "CREATE INDEX IX_CleanedGrants_CloseDate ON CleanedGrantsLayer2(CloseDate);",
        "CREATE INDEX IX_CleanedGrants_AwardCeiling ON CleanedGrantsLayer2(AwardCeiling);",
        "CREATE INDEX IX_BusinessView_Status ON GrantBusinessViewLayer3(Status);",
        "CREATE INDEX IX_BusinessView_FundingTier ON GrantBusinessViewLayer3(FundingTier);",
        "CREATE INDEX IX_BusinessView_OpportunityScore ON GrantBusinessViewLayer3(OpportunityScore);"
    ]
    
    for index_sql in indexes:
        try:
            cursor.execute(index_sql)
            index_name = index_sql.split()[2]  # Extract index name
            created_objects.append(f"Index: {index_name}")
        except Exception as e:
            logging.warning(f"Index creation warning: {str(e)}")
    
    return created_objects

def create_all_procedures(cursor) -> list:
    """Create stored procedures"""
    created_objects = []
    
    # RefreshBusinessViews procedure
    refresh_business_views_sql = """
    IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'RefreshBusinessViews')
    DROP PROCEDURE RefreshBusinessViews;
    
    CREATE PROCEDURE RefreshBusinessViews
    AS
    BEGIN
        SET NOCOUNT ON;
        
        -- Clear existing business view data
        TRUNCATE TABLE GrantBusinessViewLayer3;
        
        -- Repopulate with fresh data
        INSERT INTO GrantBusinessViewLayer3 (
            GrantID, OpportunityID, Title, Description, ShortDescription,
            PostedDate, CloseDate, AwardCeiling, AwardFloor, EstimatedTotalFunding,
            ExpectedAwards, AgencyID, AgencyName, AgencyType, Department,
            CategoryID, CategoryName, CategoryGroup, EligibilityRequirements,
            GrantsGovURL, AdditionalInfoURL, DataQualityScore
        )
        SELECT 
            g.GrantID, g.OpportunityID, g.Title, g.Description, g.ShortDescription,
            g.PostedDate, g.CloseDate, g.AwardCeiling, g.AwardFloor, g.EstimatedTotalFunding,
            g.ExpectedAwards, g.AgencyID, a.AgencyName, a.AgencyType, a.Department,
            g.CategoryID, c.CategoryName, c.CategoryGroup, g.EligibilityRequirements,
            g.GrantsGovURL, g.AdditionalInfoURL, g.DataQualityScore
        FROM CleanedGrantsLayer2 g
        LEFT JOIN AgencyMasterLayer2 a ON g.AgencyID = a.AgencyID
        LEFT JOIN CategoryMasterLayer2 c ON g.CategoryID = c.CategoryID
        WHERE g.IsActive = 1;
        
        -- Update calculated business fields
        UPDATE GrantBusinessViewLayer3 SET
            FundingTier = CASE 
                WHEN AwardCeiling >= 1000000 THEN 'High'
                WHEN AwardCeiling >= 100000 THEN 'Medium'
                ELSE 'Low'
            END,
            IsHighValue = CASE WHEN AwardCeiling >= 1000000 THEN 1 ELSE 0 END,
            Status = CASE 
                WHEN CloseDate < GETDATE() THEN 'Closed'
                WHEN DATEDIFF(day, GETDATE(), CloseDate) <= 7 THEN 'Closing Soon'
                WHEN DATEDIFF(day, GETDATE(), CloseDate) <= 30 THEN 'Closing This Month'
                ELSE 'Open'
            END,
            UrgencyLevel = CASE 
                WHEN DATEDIFF(day, GETDATE(), CloseDate) <= 3 THEN 'Critical'
                WHEN DATEDIFF(day, GETDATE(), CloseDate) <= 7 THEN 'High'
                WHEN DATEDIFF(day, GETDATE(), CloseDate) <= 14 THEN 'Medium'
                ELSE 'Low'
            END,
            OpportunityScore = CASE 
                WHEN AwardCeiling >= 1000000 AND DATEDIFF(day, GETDATE(), CloseDate) > 30 THEN 95.0
                WHEN AwardCeiling >= 500000 AND DATEDIFF(day, GETDATE(), CloseDate) > 14 THEN 85.0
                WHEN AwardCeiling >= 100000 THEN 75.0
                ELSE 65.0
            END;
    END;
    """
    
    cursor.execute(refresh_business_views_sql)
    created_objects.append("Procedure: RefreshBusinessViews")
    
    # RefreshAgencyStats procedure
    refresh_agency_stats_sql = """
    IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'RefreshAgencyStats')
    DROP PROCEDURE RefreshAgencyStats;
    
    CREATE PROCEDURE RefreshAgencyStats
    AS
    BEGIN
        SET NOCOUNT ON;
        
        MERGE AgencyStatsLayer3 AS target
        USING (
            SELECT 
                a.AgencyID,
                a.AgencyName,
                COUNT(g.GrantID) as TotalGrants,
                SUM(CASE WHEN g.CloseDate >= GETDATE() THEN 1 ELSE 0 END) as ActiveGrants,
                SUM(CASE WHEN g.CloseDate < GETDATE() THEN 1 ELSE 0 END) as ClosedGrants,
                ISNULL(SUM(g.AwardCeiling), 0) as TotalFunding,
                ISNULL(AVG(g.AwardCeiling), 0) as AvgFunding,
                ISNULL(MAX(g.AwardCeiling), 0) as MaxFunding,
                ISNULL(MIN(g.AwardCeiling), 0) as MinFunding
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
                TotalFunding = source.TotalFunding,
                AvgFunding = source.AvgFunding,
                MaxFunding = source.MaxFunding,
                MinFunding = source.MinFunding,
                LastUpdated = GETUTCDATE()
        
        WHEN NOT MATCHED THEN
            INSERT (AgencyID, AgencyName, TotalGrants, ActiveGrants, ClosedGrants, 
                    TotalFunding, AvgFunding, MaxFunding, MinFunding)
            VALUES (source.AgencyID, source.AgencyName, source.TotalGrants, 
                    source.ActiveGrants, source.ClosedGrants, source.TotalFunding,
                    source.AvgFunding, source.MaxFunding, source.MinFunding);
    END;
    """
    
    cursor.execute(refresh_agency_stats_sql)
    created_objects.append("Procedure: RefreshAgencyStats")
    
    # RefreshCategoryStats procedure
    refresh_category_stats_sql = """
    IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'RefreshCategoryStats')
    DROP PROCEDURE RefreshCategoryStats;
    
    CREATE PROCEDURE RefreshCategoryStats
    AS
    BEGIN
        SET NOCOUNT ON;
        
        MERGE CategoryStatsLayer3 AS target
        USING (
            SELECT 
                c.CategoryID,
                c.CategoryName,
                COUNT(g.GrantID) as TotalGrants,
                SUM(CASE WHEN g.CloseDate >= GETDATE() THEN 1 ELSE 0 END) as ActiveGrants,
                ISNULL(AVG(g.AwardCeiling), 0) as AvgFunding,
                ISNULL(SUM(g.AwardCeiling), 0) as TotalFunding,
                COUNT(DISTINCT g.AgencyID) as AgencyCount
            FROM CategoryMasterLayer2 c
            LEFT JOIN CleanedGrantsLayer2 g ON c.CategoryID = g.CategoryID AND g.IsActive = 1
            GROUP BY c.CategoryID, c.CategoryName
        ) AS source ON target.CategoryID = source.CategoryID
        
        WHEN MATCHED THEN
            UPDATE SET
                CategoryName = source.CategoryName,
                TotalGrants = source.TotalGrants,
                ActiveGrants = source.ActiveGrants,
                AvgFunding = source.AvgFunding,
                TotalFunding = source.TotalFunding,
                AgencyCount = source.AgencyCount,
                LastUpdated = GETUTCDATE()
        
        WHEN NOT MATCHED THEN
            INSERT (CategoryID, CategoryName, TotalGrants, ActiveGrants, 
                    AvgFunding, TotalFunding, AgencyCount)
            VALUES (source.CategoryID, source.CategoryName, source.TotalGrants, 
                    source.ActiveGrants, source.AvgFunding, source.TotalFunding,
                    source.AgencyCount);
    END;
    """
    
    cursor.execute(refresh_category_stats_sql)
    created_objects.append("Procedure: RefreshCategoryStats")
    
    return created_objects

def generate_html_response(result: Dict, operation: str) -> func.HttpResponse:
    """Generate HTML response"""
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>SQL Schema Creator - {operation.replace('_', ' ').title()}</title>
        <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
        <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    </head>
    <body class="bg-gradient-to-br from-blue-50 to-indigo-100 min-h-screen">
        <div class="container mx-auto px-4 py-8">
            <h1 class="text-4xl font-bold text-center mb-8 text-blue-600">
                <i class="fas fa-database mr-3"></i>SQL Schema Creator
            </h1>
            
            <div class="bg-white rounded-xl shadow-lg p-6 mb-8">
                <h2 class="text-2xl font-semibold mb-4 text-gray-800">
                    Operation: {operation.replace('_', ' ').title()}
                </h2>
                
                <div class="bg-gray-900 text-green-400 p-4 rounded-lg font-mono text-sm overflow-auto max-h-96">
                    <pre>{json.dumps(result, default=str, indent=2)}</pre>
                </div>
            </div>
            
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                <a href="?operation=test_connection&format=html" 
                   class="bg-blue-500 hover:bg-blue-600 text-white px-4 py-3 rounded-lg text-center block transition-all">
                    <i class="fas fa-plug mr-2"></i>Test Connection
                </a>
                <a href="?operation=create_all&format=html" 
                   class="bg-green-500 hover:bg-green-600 text-white px-4 py-3 rounded-lg text-center block transition-all">
                    <i class="fas fa-database mr-2"></i>Create All Schema
                </a>
                <a href="?operation=create_layer1&format=html" 
                   class="bg-purple-500 hover:bg-purple-600 text-white px-4 py-3 rounded-lg text-center block transition-all">
                    <i class="fas fa-layer-group mr-2"></i>Create Layer 1
                </a>
                <a href="?operation=create_layer2&format=html" 
                   class="bg-indigo-500 hover:bg-indigo-600 text-white px-4 py-3 rounded-lg text-center block transition-all">
                    <i class="fas fa-layer-group mr-2"></i>Create Layer 2
                </a>
                <a href="?operation=create_layer3&format=html" 
                   class="bg-orange-500 hover:bg-orange-600 text-white px-4 py-3 rounded-lg text-center block transition-all">
                    <i class="fas fa-layer-group mr-2"></i>Create Layer 3
                </a>
                <a href="?operation=create_procedures&format=html" 
                   class="bg-red-500 hover:bg-red-600 text-white px-4 py-3 rounded-lg text-center block transition-all">
                    <i class="fas fa-cogs mr-2"></i>Create Procedures
                </a>
            </div>
        </div>
    </body>
    </html>
    """
    
    return func.HttpResponse(html_content, mimetype="text/html")

def create_layer1_tables() -> Dict:
    """Create only Layer 1 tables"""
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        created_objects = execute_layer1_sql(cursor)
        
        connection.commit()
        connection.close()
        
        return {
            "status": "success",
            "message": "Layer 1 (Raw Data) tables created successfully",
            "created_objects": created_objects,
            "layer": "Layer 1 - Raw Data Tables"
        }
    except Exception as e:
        return {"error": f"Layer 1 creation failed: {str(e)}"}

def create_layer2_tables() -> Dict:
    """Create only Layer 2 tables"""
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        created_objects = execute_layer2_sql(cursor)
        
        connection.commit()
        connection.close()
        
        return {
            "status": "success",
            "message": "Layer 2 (Cleaned & Normalized Data) tables created successfully",
            "created_objects": created_objects,
            "layer": "Layer 2 - Cleaned & Normalized Data"
        }
    except Exception as e:
        return {"error": f"Layer 2 creation failed: {str(e)}"}

def create_layer3_tables() -> Dict:
    """Create only Layer 3 tables"""
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        created_objects = execute_layer3_sql(cursor)
        
        connection.commit()
        connection.close()
        
        return {
            "status": "success",
            "message": "Layer 3 (Business Views & Analytics) tables created successfully",
            "created_objects": created_objects,
            "layer": "Layer 3 - Business Views & Analytics"
        }
    except Exception as e:
        return {"error": f"Layer 3 creation failed: {str(e)}"}

def create_stored_procedures() -> Dict:
    """Create only stored procedures"""
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        created_objects = create_all_procedures(cursor)
        
        connection.commit()
        connection.close()
        
        return {
            "status": "success",
            "message": "Stored procedures created successfully",
            "created_objects": created_objects,
            "procedures": "Data processing and analytics procedures"
        }
    except Exception as e:
        return {"error": f"Procedure creation failed: {str(e)}"}