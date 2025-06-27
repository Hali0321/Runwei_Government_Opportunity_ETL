
        -- ================================================
        -- ENHANCED HTML CLEANUP - DIRECT INTEGRATION
        -- Remove ALL HTML tags and entities for pure text
        -- ================================================

        BEGIN TRANSACTION EnhancedHTMLCleanup;

        -- PHASE 1: HTML TAG REMOVAL
        -- Remove paragraph tags and convert to line breaks
        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '<p>', CHAR(10))
        WHERE Description LIKE '%<p>%';

        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '</p>', CHAR(10))
        WHERE Description LIKE '%</p>%';

        -- Remove line break tags and convert to actual line breaks
        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '<br>', CHAR(10))
        WHERE Description LIKE '%<br>%';

        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '<br/>', CHAR(10))
        WHERE Description LIKE '%<br/>%';

        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '<br />', CHAR(10))
        WHERE Description LIKE '%<br />%';

        -- Remove strong/bold tags but keep content
        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '<strong>', '')
        WHERE Description LIKE '%<strong>%';

        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '</strong>', '')
        WHERE Description LIKE '%</strong>%';

        -- Remove emphasis tags
        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '<em>', '')
        WHERE Description LIKE '%<em>%';

        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '</em>', '')
        WHERE Description LIKE '%</em>%';

        -- Remove list tags and convert to bullets
        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '<ul>', CHAR(10))
        WHERE Description LIKE '%<ul>%';

        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '</ul>', CHAR(10))
        WHERE Description LIKE '%</ul>%';

        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '<li>', CHAR(10) + '• ')
        WHERE Description LIKE '%<li>%';

        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '</li>', '')
        WHERE Description LIKE '%</li>%';

        -- PHASE 2: COMPREHENSIVE SPAN TAG REMOVAL
        -- Remove span closing tags first
        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '</span>', '')
        WHERE Description LIKE '%</span>%';

        -- Remove span tags with style attributes (iterative approach)
        DECLARE @MaxIterations INT = 100;
        DECLARE @CurrentIteration INT = 0;
        DECLARE @RowsUpdated INT = 1;

        WHILE @RowsUpdated > 0 AND @CurrentIteration < @MaxIterations
        BEGIN
            SET @CurrentIteration = @CurrentIteration + 1;
            
            UPDATE CleanGrantsLayer2 
            SET Description = 
                CASE 
                    WHEN CHARINDEX('<span', Description) > 0 AND CHARINDEX('>', Description, CHARINDEX('<span', Description)) > 0 THEN
                        LEFT(Description, CHARINDEX('<span', Description) - 1) +
                        SUBSTRING(Description, CHARINDEX('>', Description, CHARINDEX('<span', Description)) + 1, LEN(Description))
                    ELSE Description
                END
            WHERE Description LIKE '%<span%';
            
            SET @RowsUpdated = @@ROWCOUNT;
        END;

        -- PHASE 3: LINK CLEANUP
        -- Remove link tags but preserve content
        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '<a href="', '')
        WHERE Description LIKE '%<a href="%';

        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '" target="_blank">', '')
        WHERE Description LIKE '%target="_blank">%';

        UPDATE CleanGrantsLayer2 
        SET Description = REPLACE(Description, '</a>', '')
        WHERE Description LIKE '%</a>%';

        -- PHASE 4: STYLE ATTRIBUTE REMOVAL
        SET @CurrentIteration = 0;
        SET @RowsUpdated = 1;

        WHILE @RowsUpdated > 0 AND @CurrentIteration < @MaxIterations
        BEGIN
            SET @CurrentIteration = @CurrentIteration + 1;
            
            UPDATE CleanGrantsLayer2 
            SET Description = 
                CASE 
                    WHEN CHARINDEX('style="', Description) > 0 THEN
                        LEFT(Description, CHARINDEX('style="', Description) - 1) +
                        SUBSTRING(Description, 
                            CHARINDEX('"', Description, CHARINDEX('style="', Description) + 7) + 1, 
                            LEN(Description))
                    ELSE Description
                END
            WHERE Description LIKE '%style="%';
            
            SET @RowsUpdated = @@ROWCOUNT;
        END;

        -- PHASE 5: HTML ENTITY CLEANUP
        UPDATE CleanGrantsLayer2 SET Description = REPLACE(Description, '&nbsp;', ' ') WHERE Description LIKE '%&nbsp;%';
        UPDATE CleanGrantsLayer2 SET Description = REPLACE(Description, '&amp;', '&') WHERE Description LIKE '%&amp;%';
        UPDATE CleanGrantsLayer2 SET Description = REPLACE(Description, '&quot;', '"') WHERE Description LIKE '%&quot;%';
        UPDATE CleanGrantsLayer2 SET Description = REPLACE(Description, '&lt;', '<') WHERE Description LIKE '%&lt;%';
        UPDATE CleanGrantsLayer2 SET Description = REPLACE(Description, '&gt;', '>') WHERE Description LIKE '%&gt;%';
        UPDATE CleanGrantsLayer2 SET Description = REPLACE(Description, '&rsquo;', '''') WHERE Description LIKE '%&rsquo;%';
        UPDATE CleanGrantsLayer2 SET Description = REPLACE(Description, '&lsquo;', '''') WHERE Description LIKE '%&lsquo;%';
        UPDATE CleanGrantsLayer2 SET Description = REPLACE(Description, '&rdquo;', '"') WHERE Description LIKE '%&rdquo;%';
        UPDATE CleanGrantsLayer2 SET Description = REPLACE(Description, '&ldquo;', '"') WHERE Description LIKE '%&ldquo;%';
        UPDATE CleanGrantsLayer2 SET Description = REPLACE(Description, '&mdash;', ' - ') WHERE Description LIKE '%&mdash;%';
        UPDATE CleanGrantsLayer2 SET Description = REPLACE(Description, '&ndash;', '-') WHERE Description LIKE '%&ndash;%';

        -- PHASE 6: GENERIC HTML TAG REMOVAL (catch-all)
        SET @CurrentIteration = 0;
        SET @RowsUpdated = 1;

        WHILE @RowsUpdated > 0 AND @CurrentIteration < @MaxIterations
        BEGIN
            SET @CurrentIteration = @CurrentIteration + 1;
            
            UPDATE CleanGrantsLayer2 
            SET Description = 
                CASE 
                    WHEN CHARINDEX('<', Description) > 0 AND CHARINDEX('>', Description, CHARINDEX('<', Description)) > 0 THEN
                        LEFT(Description, CHARINDEX('<', Description) - 1) +
                        SUBSTRING(Description, CHARINDEX('>', Description, CHARINDEX('<', Description)) + 1, LEN(Description))
                    ELSE Description
                END
            WHERE Description LIKE '%<%>%';
            
            SET @RowsUpdated = @@ROWCOUNT;
        END;

        -- PHASE 7: PROFESSIONAL SPACING
        -- Remove excessive spaces
        UPDATE CleanGrantsLayer2 SET Description = REPLACE(Description, '  ', ' ') WHERE Description LIKE '%  %';
        UPDATE CleanGrantsLayer2 SET Description = REPLACE(Description, '  ', ' ') WHERE Description LIKE '%  %';
        UPDATE CleanGrantsLayer2 SET Description = REPLACE(Description, '  ', ' ') WHERE Description LIKE '%  %';

        -- PHASE 8: FINAL CLEANUP
        -- Trim whitespace
        UPDATE CleanGrantsLayer2 SET Description = LTRIM(RTRIM(Description)) WHERE Description IS NOT NULL;

        -- Update quality scores
        UPDATE CleanGrantsLayer2
        SET UpdatedDate = GETDATE(),
            ProcessedBy = 'Enhanced_Layer2_HTML_Cleanup',
            DataQualityScore = 
                CASE 
                    WHEN LEN(Description) >= 100 
                        AND Description NOT LIKE '%<%' 
                        AND Description NOT LIKE '%&%;%' 
                        AND ASCII(LEFT(Description, 1)) >= 65 THEN 98.0
                    WHEN LEN(Description) >= 50 
                        AND Description NOT LIKE '%<%' THEN 90.0
                    WHEN LEN(Description) >= 25 THEN 80.0
                    ELSE 70.0
                END
        WHERE Description IS NOT NULL;

        COMMIT TRANSACTION EnhancedHTMLCleanup;

        -- Verification
        SELECT 
            'HTML_CLEANUP_VERIFICATION' as Status,
            COUNT(*) as Total_Records,
            COUNT(CASE WHEN Description LIKE '%<%' THEN 1 END) as Records_Still_With_HTML,
            COUNT(CASE WHEN Description LIKE '%<span%' THEN 1 END) as Records_Still_With_Spans,
            COUNT(CASE WHEN Description LIKE '%&%;%' THEN 1 END) as Records_Still_With_Entities,
            ROUND(AVG(DataQualityScore), 2) as Average_Quality_Score,
            CASE 
                WHEN COUNT(CASE WHEN Description LIKE '%<%' OR Description LIKE '%&%;%' THEN 1 END) = 0 
                THEN '✅ HTML CLEANUP COMPLETE!'
                ELSE '⚠️ Some HTML may remain'
            END as Cleanup_Status
        FROM CleanGrantsLayer2;

        -- Show clean examples
        SELECT TOP 3
            OpportunityNumber,
            LEFT(Description, 200) as Clean_Description_Sample
        FROM CleanGrantsLayer2
        WHERE DataQualityScore >= 90.0
        ORDER BY DataQualityScore DESC;
        