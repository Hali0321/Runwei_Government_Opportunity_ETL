-- ===================================
-- GRANTS.GOV API AZURE - REFERENCE DATA
-- ===================================

USE GrantsGovDB;
GO

-- Insert Agency Reference Data
INSERT INTO AgencyMasterLayer2 (AgencyCode, AgencyName, ParentAgency, WebsiteURL, DataQualityScore, IsActive)
VALUES 
('HHS', 'Department of Health and Human Services', NULL, 'https://www.hhs.gov', 95, 1),
('NIH', 'National Institutes of Health', 'HHS', 'https://www.nih.gov', 97, 1),
('NSF', 'National Science Foundation', NULL, 'https://www.nsf.gov', 96, 1),
('ED', 'Department of Education', NULL, 'https://www.ed.gov', 92, 1),
('DOE', 'Department of Energy', NULL, 'https://www.energy.gov', 94, 1),
('USDA', 'Department of Agriculture', NULL, 'https://www.usda.gov', 93, 1),
('DOD', 'Department of Defense', NULL, 'https://www.defense.gov', 91, 1),
('CDC', 'Centers for Disease Control and Prevention', 'HHS', 'https://www.cdc.gov', 96, 1);
GO

-- Insert Category Reference Data
INSERT INTO CategoryMasterLayer2 (CategoryName, CategoryGroup, Keywords, IsActive)
VALUES 
('Health', 'Medical and Health Sciences', 'healthcare, medicine, wellness, medical research, public health', 1),
('Science and Technology', 'STEM', 'research, innovation, engineering, computing, biology', 1),
('Education', 'Education and Training', 'schools, universities, teaching, learning, curriculum', 1),
('Agriculture', 'Food and Agriculture', 'farming, agriculture, food production, rural development', 1),
('Energy', 'Energy and Environment', 'renewable energy, sustainability, conservation, climate', 1),
('Infrastructure', 'Public Works and Infrastructure', 'roads, bridges, construction, transportation, facilities', 1),
('Community Development', 'Social Services', 'housing, urban development, poverty reduction, social welfare', 1),
('Arts and Humanities', 'Culture and Arts', 'museums, literature, performing arts, cultural heritage', 1);
GO

-- Insert Eligibility Reference Data
INSERT INTO EligibilityMasterLayer2 (EligibilityType, EligibilityDescription, IsActive)
VALUES 
('Public/State Institutions', 'Public and State controlled institutions of higher education', 1),
('Private Institutions', 'Private institutions of higher education', 1),
('Nonprofit Organizations', '501(c)(3) nonprofit organizations with valid tax-exempt status', 1),
('For-profit Organizations', 'Small businesses and for-profit organizations', 1),
('State Governments', 'State governments and state agencies', 1),
('Local Governments', 'City or township governments, county governments', 1),
('Tribal Governments', 'Native American tribal governments and organizations', 1),
('Special District Governments', 'Special district governments', 1),
('Individual Researchers', 'Individual researchers with institutional affiliations', 1);
GO

-- Insert Geographic Coverage Reference Data
INSERT INTO GeographicCoverageMasterLayer2 (GeographicLevel, RegionName, IsActive)
VALUES 
('National', 'United States', 1),
('Region', 'Northeast', 1),
('Region', 'Southeast', 1),
('Region', 'Midwest', 1),
('Region', 'Southwest', 1),
('Region', 'West', 1),
('State', 'California', 1),
('State', 'Texas', 1),
('State', 'New York', 1),
('State', 'Florida', 1),
('Urban', 'Metropolitan Areas', 1),
('Rural', 'Rural Communities', 1),
('Tribal', 'Tribal Lands', 1),
('International', 'International Components', 1);
GO