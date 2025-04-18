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
    ) AS clustering_columns,
    COUNT(CASE WHEN clustering_ordinal_position IS NOT NULL THEN 1 ELSE NULL END) AS clustering_columns_count
  FROM
    `<PROJECT_ID>.region-<REGION>`.INFORMATION_SCHEMA.COLUMNS
  GROUP BY
    project_id,
    dataset_id,
    table_name
  HAVING
    MAX(CASE WHEN is_partitioning_column = 'YES' THEN 1 ELSE 0 END) = 1
    OR COUNT(CASE WHEN clustering_ordinal_position IS NOT NULL THEN 1 ELSE NULL END) > 0
)

SELECT
  m.project_id,
  m.dataset_id,
  m.table_name,
  m.partition_column,
  m.clustering_columns,
  m.clustering_columns_count,
  s.total_logical_bytes AS size_bytes,
  ROUND(s.total_logical_bytes / POW(1024, 3), 2) AS size_gb,
  s.total_rows,
  s.total_partitions
FROM
  table_metadata m
JOIN
  `<PROJECT_ID>.region-<REGION>`.INFORMATION_SCHEMA.TABLE_STORAGE s
ON
  m.project_id = s.project_id AND
  m.dataset_id = s.table_schema AND
  m.table_name = s.table_name
ORDER BY
  m.project_id,
  m.dataset_id,
  m.table_name;
