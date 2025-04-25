SELECT 
    opportunity_title AS title,

    '' AS service_provider_eso,
    '' AS eso_website,
    '' AS cover_image,
    'No' AS global_opportunity,
    '' AS global_locations,

    opportunity_link AS url,
    opportunity_link AS direct_apply_link,

    CASE
        WHEN award_ceiling IS NULL OR award_ceiling::text ILIKE 'NaN' THEN '0'
        ELSE award_ceiling::text
    END AS award_value_str,

    CASE
        WHEN award_ceiling IS NULL OR award_ceiling::text ILIKE 'NaN' THEN 0
        ELSE CAST(award_ceiling AS INTEGER)
    END AS cash_award,

    post_date AS date_posted,

    CASE 
        WHEN close_date IS NULL THEN 'Yes'
        ELSE 'No'
    END AS is_rolling,

    CASE 
        WHEN close_date IS NULL THEN ''
        ELSE TO_CHAR(close_date, 'YYYY-MM-DD')
    END AS deadline_str,

    '' AS opportunitytype,
    '' AS industry,
    '' AS tags,
    '' AS sdg_alignment,
    '' AS opportunity_gap,

    TRIM(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                COALESCE(grantor_contact, ''),
                                '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}', 
                                '', 'gi'
                            ),
                            '[0-9]{3}[-. ][0-9]{3}[-. ][0-9]{4}', 
                            '', 'gi'
                        ),
                        'https?://[^\\s]+', 
                        '', 'gi'
                    ),
                    '(For further information[^\\n]*|If you have difficulty accessing[^\\n]*|Contact:?|Email:?|Tel:?|Telephone:?|by e-mail at\\s*)', 
                    '', 'gi'
                ),
                E'(<br\\s*/?>|&lt;br/?&gt;|\\r?\\n)', 
                E'\n', 'g'
            ),
            E'\\s+', ' ', 'g'
        )
    ) AS contact_names,

    CASE 
        WHEN grantor_email ILIKE 'NaN' OR grantor_email ILIKE 'N/A' OR grantor_email IS NULL THEN ''
        ELSE grantor_email
    END AS contact_email

FROM grants_data
WHERE 
    (
        close_date >= CURRENT_DATE
        OR (
            close_date IS NULL 
            AND EXTRACT(YEAR FROM last_updated_date) = EXTRACT(YEAR FROM CURRENT_DATE)
        )
    )
    AND (archive_date IS NULL OR archive_date > CURRENT_DATE)
    AND cost_sharing_required = FALSE
	LIMIT 10;













-- This is for Viewing --
SELECT 
    post_date, 
    close_date, 
    last_updated_date, 
    archive_date, 
    cost_sharing_required, 
    opportunity_title
FROM grants_data
WHERE 
    (
        close_date >= CURRENT_DATE
        OR (
            close_date IS NULL 
            AND EXTRACT(YEAR FROM last_updated_date) = EXTRACT(YEAR FROM CURRENT_DATE)
        )
    )
    AND (archive_date IS NULL OR archive_date > CURRENT_DATE)
    AND cost_sharing_required = FALSE;
