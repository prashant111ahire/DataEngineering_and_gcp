WITH table_metadata AS (
  SELECT
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name,
    MAX(CASE WHEN is_partitioning_column = 'YES' THEN column_name ELSE NULL END) AS partition_column,
    STRING_AGG(
      CASE WHEN clustering_ordinal_position IS NOT NULL
           THEN column_name
           ELSE NULL
      END,
      ', '
      ORDER BY clustering_ordinal_position
    ) AS clustering_columns,
    COUNT(CASE WHEN clustering_ordinal_position IS NOT NULL THEN 1 ELSE NULL END) AS clustering_columns_count
  FROM
   `<PROJECT_ID>.region-<REGION>`.INFORMATION_SCHEMA.COLUMNS
  GROUP BY
    project_id,
    dataset_id,
    table_name
),
all_tables AS (
  SELECT
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name
  FROM
    `<PROJECT_ID>.region-<REGION>`.INFORMATION_SCHEMA.TABLES
)
SELECT
  a.project_id,
  CONCAT(a.dataset_id, a.table_name) dataset_table_id,
  a.dataset_id DATASET_ID,
  a.table_name TABLE_ID,
  m.partition_column PARTITION_COL,
  m.clustering_columns CLUSTER_COL,
  m.clustering_columns_count CLUSTER_COL_COUNT,
  s.total_logical_bytes AS SIZE_BYTES,
  ROUND(s.total_logical_bytes / POW(1024, 3), 2) AS SIZE_GB,
  ROUND(s.total_logical_bytes / POW(1024, 4), 2) AS SIZE_TB,
  s.total_rows TOTAL_ROWS,
  s.total_partitions AS TOTAL_PARTITIONS,
  date(s.STORAGE_LAST_MODIFIED_TIME) LAST_MODIFIED_DATE,
concat(cast(Extract(year from s.STORAGE_LAST_MODIFIED_TIME) as string),'-',cast(Extract(month from s.STORAGE_LAST_MODIFIED_TIME) as string)) LAST_MODIFIED_MONTH_YR
FROM
  all_tables a
LEFT JOIN
  table_metadata m
ON
  a.project_id = m.project_id AND
  a.dataset_id = m.dataset_id AND
  a.table_name = m.table_name
JOIN
  `<PROJECT_ID>.region-<REGION>`.INFORMATION_SCHEMA.TABLE_STORAGE s
ON
  a.project_id = s.project_id AND
  a.dataset_id = s.table_schema AND
  a.table_name = s.table_name
ORDER BY
  a.project_id,
  a.dataset_id,
  a.table_name;
