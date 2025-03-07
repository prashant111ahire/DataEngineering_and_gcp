WITH table_1_columns AS (
  SELECT 
    field_path AS field_path_1,
    data_type AS data_type_1
  FROM `<PROJECT_ID>.<REGION>.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
  WHERE table_name = '<TABLE_NAME>'
    AND table_schema = 'TABLE_SCHEMA'
),
table_2_columns AS (
  SELECT 
    field_path AS field_path_2,
    data_type AS data_type_2
  FROM `<PROJECT_ID>.<REGION>.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
  WHERE table_name = '<TABLE_NAME>'
    AND table_schema = 'TABLE_SCHEMA'
)
SELECT 
  COALESCE(t1.field_path_1, t2.field_path_2) AS field_path,
  t1.data_type_1 AS table_1_data_type,
  t2.data_type_2 AS table_2_data_type,
  CASE
    WHEN t1.field_path_1 IS NULL THEN 'Missing in Table 1'
    WHEN t2.field_path_2 IS NULL THEN 'Missing in Table 2'
    WHEN lower(t1.data_type_1) != lower(t2.data_type_2) THEN 'Data type mismatch'
    ELSE 'Match'
  END AS status
FROM table_1_columns t1
FULL OUTER JOIN table_2_columns t2
  ON LOWER(t1.field_path_1) = LOWER(t2.field_path_2)
ORDER BY field_path;
