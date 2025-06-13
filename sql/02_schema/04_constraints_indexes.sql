-- ===================================
-- GRANTS.GOV API AZURE - CONSTRAINTS & INDEXES
-- ===================================

USE GrantsGovDB;
GO

-- Add self-referencing foreign key to CategoryMasterLayer2
IF EXISTS (SELECT * FROM sysobjects WHERE name='CategoryMasterLayer2' AND xtype='U')
AND NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_Category_Parent')
BEGIN
    ALTER TABLE CategoryMasterLayer2 
    ADD CONSTRAINT FK_Category_Parent 
    FOREIGN KEY (ParentCategoryID) REFERENCES CategoryMasterLayer2(CategoryID);
    PRINT '✅ Added parent category self-reference constraint';
END

-- Add foreign keys to CleanedGrantsLayer2
IF EXISTS (SELECT * FROM sysobjects WHERE name='CleanedGrantsLayer2' AND xtype='U')
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_CleanedGrants_Agency')
    BEGIN
        ALTER TABLE CleanedGrantsLayer2 
        ADD CONSTRAINT FK_CleanedGrants_Agency 
        FOREIGN KEY (AgencyID) REFERENCES AgencyMasterLayer2(AgencyID);
        PRINT '✅ Added FK_CleanedGrants_Agency foreign key constraint';
    END

    IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_CleanedGrants_Category')
    BEGIN
        ALTER TABLE CleanedGrantsLayer2 
        ADD CONSTRAINT FK_CleanedGrants_Category 
        FOREIGN KEY (CategoryID) REFERENCES CategoryMasterLayer2(CategoryID);
        PRINT '✅ Added FK_CleanedGrants_Category foreign key constraint';
    END
END

-- Add foreign key to GrantBusinessViewLayer3
IF EXISTS (SELECT * FROM sysobjects WHERE name='GrantBusinessViewLayer3' AND xtype='U')
AND NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_BusinessView_Grant')
BEGIN
    ALTER TABLE GrantBusinessViewLayer3 
    ADD CONSTRAINT FK_BusinessView_Grant
    FOREIGN KEY (GrantID) REFERENCES CleanedGrantsLayer2(GrantID);
    PRINT '✅ Added FK_BusinessView_Grant foreign key constraint';
END

-- Add foreign key to AgencyStatsLayer3
IF EXISTS (SELECT * FROM sysobjects WHERE name='AgencyStatsLayer3' AND xtype='U')
AND NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_AgencyStats_Agency')
BEGIN
    ALTER TABLE AgencyStatsLayer3 
    ADD CONSTRAINT FK_AgencyStats_Agency
    FOREIGN KEY (AgencyID) REFERENCES AgencyMasterLayer2(AgencyID);
    PRINT '✅ Added FK_AgencyStats_Agency foreign key constraint';
END

-- Add foreign key to CategoryStatsLayer3
IF EXISTS (SELECT * FROM sysobjects WHERE name='CategoryStatsLayer3' AND xtype='U')
AND NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_CategoryStats_Category')
BEGIN
    ALTER TABLE CategoryStatsLayer3 
    ADD CONSTRAINT FK_CategoryStats_Category
    FOREIGN KEY (CategoryID) REFERENCES CategoryMasterLayer2(CategoryID);
    PRINT '✅ Added FK_CategoryStats_Category foreign key constraint';
END

PRINT '✅ All constraints and indexes created successfully';
GO
