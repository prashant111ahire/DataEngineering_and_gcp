-- Query to compare column names and data types of two tables
WITH table_1_columns AS (
  SELECT 
    column_name AS column_name_1,
    data_type AS data_type_1
  FROM `jwn-nap-dataplex-prod-emue.region-us-west1.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'customer_activity_added_to_bag'
  and table_schema='ACP_EVENT_VIEW'
),
table_2_columns AS (
  SELECT 
    column_name AS column_name_2,
    data_type AS data_type_2
  FROM `jwn-nap-dataplex-nonprod-lpyu.region-us-west1.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'customer_activity_added_to_bag_parquet'
  and table_schema='acp_event'
)
SELECT 
  COALESCE(t1.column_name_1, t2.column_name_2) AS column_name,
  t1.data_type_1 AS table_1_data_type,
  t2.data_type_2 AS table_2_data_type,
  CASE
    WHEN t1.column_name_1 IS NULL THEN 'Missing in Table 1'
    WHEN t2.column_name_2 IS NULL THEN 'Missing in Table 2'
    WHEN t1.data_type_1 != t2.data_type_2 THEN 'Data type mismatch'
    ELSE 'Match'
  END AS status
FROM table_1_columns t1
FULL OUTER JOIN table_2_columns t2
ON LOWER(t1.column_name_1) = LOWER(t2.column_name_2) -- Case-insensitive comparison
ORDER BY column_name;
