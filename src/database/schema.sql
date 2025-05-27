CREATE TABLE Grants (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    OpportunityId NVARCHAR(50) NOT NULL,
    Title NVARCHAR(255) NOT NULL,
    Description NVARCHAR(MAX),
    Agency NVARCHAR(255),
    OpenDate DATETIME,
    CloseDate DATETIME,
    AwardCeiling DECIMAL(18,2),
    ProcessedDate DATETIME DEFAULT GETDATE(),
    CONSTRAINT UQ_OpportunityId UNIQUE (OpportunityId)
);

CREATE TABLE GrantEligibility (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    GrantId INT NOT NULL,
    EligibilityType NVARCHAR(100) NOT NULL,
    FOREIGN KEY (GrantId) REFERENCES Grants(Id)
);

CREATE TABLE GrantCategories (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    GrantId INT NOT NULL,
    CategoryName NVARCHAR(100) NOT NULL,
    FOREIGN KEY (GrantId) REFERENCES Grants(Id)
);
