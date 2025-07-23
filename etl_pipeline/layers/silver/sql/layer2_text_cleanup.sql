
-- Phase 2: Handle HTML tags with attributes using PATINDEX
-- This approach finds and removes tags with attributes

-- Remove span tags with class attributes
WHILE EXISTS (SELECT 1 FROM CleanGrantsLayer2 WHERE Description LIKE '%<span %')
BEGIN
    UPDATE TOP (100) CleanGrantsLayer2
    SET Description = 
        CASE 
            WHEN CHARINDEX('<span ', Description) > 0 AND CHARINDEX('>', Description, CHARINDEX('<span ', Description)) > 0
            THEN 
                STUFF(Description, 
                      CHARINDEX('<span ', Description), 
                      CHARINDEX('>', Description, CHARINDEX('<span ', Description)) - CHARINDEX('<span ', Description) + 1, 
                      '')
            ELSE Description
        END
    WHERE Description LIKE '%<span %'
END

-- Remove div tags with class attributes  
WHILE EXISTS (SELECT 1 FROM CleanGrantsLayer2 WHERE Description LIKE '%<div %')
BEGIN
    UPDATE TOP (100) CleanGrantsLayer2
    SET Description = 
        CASE 
            WHEN CHARINDEX('<div ', Description) > 0 AND CHARINDEX('>', Description, CHARINDEX('<div ', Description)) > 0
            THEN 
                STUFF(Description, 
                      CHARINDEX('<div ', Description), 
                      CHARINDEX('>', Description, CHARINDEX('<div ', Description)) - CHARINDEX('<div ', Description) + 1, 
                      '')
            ELSE Description
        END
    WHERE Description LIKE '%<div %'
END

-- Remove anchor tags with href attributes
WHILE EXISTS (SELECT 1 FROM CleanGrantsLayer2 WHERE Description LIKE '%<a %')
BEGIN
    UPDATE TOP (100) CleanGrantsLayer2
    SET Description = 
        CASE 
            WHEN CHARINDEX('<a ', Description) > 0 AND CHARINDEX('>', Description, CHARINDEX('<a ', Description)) > 0
            THEN 
                STUFF(Description, 
                      CHARINDEX('<a ', Description), 
                      CHARINDEX('>', Description, CHARINDEX('<a ', Description)) - CHARINDEX('<a ', Description) + 1, 
                      '')
            ELSE Description
        END
    WHERE Description LIKE '%<a %'
END

-- Final report
SELECT 
    'COMPLEX_CLEANUP_RESULTS' as Report_Type,
    COUNT(*) as Total_Records,
    COUNT(CASE WHEN Description LIKE '%<%' THEN 1 END) as HTML_Tags_Remaining,
    COUNT(CASE WHEN Description LIKE '%&%' THEN 1 END) as HTML_Entities_Remaining,
    ROUND(AVG(CAST(LEN(ISNULL(Description, '')) as FLOAT)), 0) as Avg_Description_Length
FROM CleanGrantsLayer2;
