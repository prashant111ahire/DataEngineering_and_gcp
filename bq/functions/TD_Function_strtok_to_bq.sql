CREATE OR REPLACE FUNCTION `test.strtok`(input_string STRING, delimiters STRING, position INT64)
RETURNS STRING AS (
(
    WITH split_result AS (
        SELECT
            ARRAY(
                SELECT TRIM(value)
                FROM UNNEST(REGEXP_EXTRACT_ALL(input_string, r'[^' || REGEXP_REPLACE(delimiters, r'([-\\.])', r'\\\1') || ']+')) AS value
            ) AS parts
    )
    SELECT IF(position > 0 AND position <= ARRAY_LENGTH(parts), parts[ORDINAL(position)], NULL)
    FROM split_result
)
);
