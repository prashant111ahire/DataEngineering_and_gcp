--This will extract source of the views with partition and clustering details
-- All the parent sources with level of hierarchy

WITH RECURSIVE base_sources AS (
  -- Step 1: Get initial set of objects (tables and views)
  SELECT
    table_schema,
    table_name,
    CASE table_type WHEN 'BASE TABLE' THEN 'Table' ELSE 'View' END AS object_type,  -- Correctly assign object type
    REGEXP_REPLACE(REGEXP_EXTRACT(ddl, r'(?i)(?:from|join)\s+([\w.`]+)'), r'[`;]', '') AS source,
    1 AS hierarchy_level
  FROM `region-us-west1`.INFORMATION_SCHEMA.TABLES
  WHERE UPPER(CONCAT(table_schema, '.', table_name)) IN (
    'PRD_NAP_VWS.ORDER_ZIP_VW','PRD_NAP_SRPT_BASE_VWS.PRODUCT_SKU_SUPP_AEDW_VW',
    'PRD_NAP_SRPT_BASE_VWS.STORE_PAYROLLDEPT_MERCHDEPT_DIM','PRD_NAP_SRPT_BASE_VWS.PRODUCT_AEDW_CLASS_DIM',
    'PRD_NAP_SRPT_BASE_VWS.PRODUCT_AEDW_DEPARTMENT_DIM','PRD_NAP_SRPT_BASE_VWS.PRODUCT_AEDW_DIVISION_DIM',
    'PRD_NAP_SRPT_BASE_VWS.PRODUCT_AEDW_GROUP_DIM','PRD_NAP_SRPT_BASE_VWS.PRODUCT_AEDW_SUBCLASS_DIM',
    'PRD_NAP_SRPT_BASE_VWS.PRODUCT_AEDW_SKU_DIM','PRD_NAP_SRPT_BASE_VWS.PRODUCT_AEDW_SKU_ATTRIBUTE_DIM',
    'PRD_NAP_SRPT_BASE_VWS.PRODUCT_AEDW_STYLE_GROUP_DIM','PRD_NAP_SRPT_BASE_VWS.PRODUCT_AEDW_SUPP_DIM'
  )

  UNION ALL

  -- Step 2: Recursively find sources of views
  SELECT
    v.table_schema,
    v.table_name,
    'View' AS object_type,
    REGEXP_REPLACE(REGEXP_EXTRACT(v.view_definition, r'(?i)(?:from|join)\s+([\w.`]+)'), r'[`;]', '') AS source,
    bs.hierarchy_level + 1 AS hierarchy_level
  FROM base_sources bs
  JOIN `region-us-west1`.INFORMATION_SCHEMA.VIEWS v
    ON UPPER(bs.source) = UPPER(CONCAT(v.table_schema, '.', v.table_name))
),

-- Step 3: Identify source object type (Table or View)
source_classification AS (
  SELECT 
    bs.table_schema,
    bs.table_name,
    bs.object_type,
    bs.source,
    bs.hierarchy_level,
    CASE 
      WHEN v.table_name IS NOT NULL THEN 'View'  -- If found in VIEWS, it's a View
      WHEN t.table_name IS NOT NULL THEN 'Table' -- If found in TABLES, it's a Table
      ELSE 'Unknown'
    END AS source_object_type
  FROM base_sources bs
  LEFT JOIN `region-us-west1`.INFORMATION_SCHEMA.VIEWS v
    ON UPPER(bs.source) = UPPER(CONCAT(v.table_schema, '.', v.table_name))
  LEFT JOIN `region-us-west1`.INFORMATION_SCHEMA.TABLES t
    ON UPPER(bs.source) = UPPER(CONCAT(t.table_schema, '.', t.table_name))
)

SELECT
  CONCAT(sc.table_schema, '.', sc.table_name) AS object_name,
  sc.object_type,
  sc.source,
  sc.source_object_type,
  sc.hierarchy_level,
  tp.source_partition_columns,
  COALESCE(tp.source_cluster_columns, 'No clustering columns') AS source_cluster_columns,
  ts.size_bytes / 1024 / 1024 / 1024 AS source_table_size_gb
FROM source_classification sc
LEFT JOIN (
  -- Table Partitioning Info
  SELECT
    table_schema,
    table_name,
    STRING_AGG(CASE WHEN is_partitioning_column = 'YES' THEN column_name END) AS source_partition_columns,
    STRING_AGG(CASE WHEN clustering_ordinal_position IS NOT NULL THEN column_name END) AS source_cluster_columns
  FROM `region-us-west1`.INFORMATION_SCHEMA.COLUMNS
  GROUP BY table_schema, table_name
) tp
ON UPPER(sc.source) = UPPER(CONCAT(tp.table_schema, '.', tp.table_name))
LEFT JOIN (
  -- Table Size Info
  SELECT
    table_schema,
    table_name,
    TOTAL_LOGICAL_BYTES AS size_bytes
  FROM `region-us-west1`.INFORMATION_SCHEMA.TABLE_STORAGE
) ts
ON UPPER(sc.source) = UPPER(CONCAT(ts.table_schema, '.', ts.table_name))
ORDER BY hierarchy_level;


------------------------------------------------
--This will extract source of the views with partition and clustering details
--WITH SOURCE TABLE SIZE

WITH base_sources AS (
  SELECT 
    table_schema,
    table_name,
    ddl,
    REGEXP_REPLACE(
      REGEXP_EXTRACT(
        ddl, 
        r'(?i)(?:from|join)\s+([\w.`]+)'  
      ), 
      r'[`;]',                             
      ''
    ) AS source,
    table_type as object_type
  FROM `region-us-west1`.INFORMATION_SCHEMA.TABLES
  WHERE UPPER(CONCAT(table_schema, '.', table_name)) IN (
 'PRD_NAP_BASE_VWS.PRODUCT_SKU_DIM_HIST','PRD_NAP_BASE_VWS.PRODUCT_STYLE_DIM_HIST','PRD_NAP_BASE_VWS.DEPARTMENT_DIM_HIST','PRD_NAP_BASE_VWS.DEPARTMENT_CLASS_SUBCLASS_DIM_HIST','PRD_NAP_BASE_VWS.VENDOR_PAYTO_RELATIONSHIP_DIM','PRD_NAP_BASE_VWS.VENDOR_LABEL_DIM','PRD_NAP_BASE_VWS.VENDOR_DIM','PRD_NAP_BASE_VWS.VENDOR_DIM','PRD_NAP_BASE_VWS.VENDOR_PAYTO_RELATIONSHIP_DIM'
  )
),
source_classification AS (
  SELECT 
    bs.table_schema,
    bs.table_name,
    bs.source,
    object_type,
    CASE 
      WHEN v.table_name IS NOT NULL THEN 'View'
      ELSE 'Table'
    END AS source_type
  FROM base_sources bs
  LEFT JOIN `region-us-west1`.INFORMATION_SCHEMA.VIEWS v
    ON UPPER(bs.source) = UPPER(CONCAT(v.table_schema, '.', v.table_name))
),
table_partitioning AS (
  SELECT 
    table_schema,
    table_name,
    STRING_AGG(CASE WHEN is_partitioning_column = 'YES' THEN column_name END) AS source_partition_columns,
    STRING_AGG(CASE WHEN clustering_ordinal_position IS NOT NULL THEN column_name END) AS source_cluster_columns
  FROM `region-us-west1`.INFORMATION_SCHEMA.COLUMNS
  WHERE UPPER(CONCAT(table_schema, '.', table_name)) IN (
    SELECT DISTINCT source 
    FROM source_classification 
    WHERE source_type = 'Table'
  )
  GROUP BY table_schema, table_name
),
object_partitioning AS (
  SELECT 
    table_schema,
    table_name,
    STRING_AGG(CASE WHEN is_partitioning_column = 'YES' THEN column_name END) AS object_partition_columns,
    STRING_AGG(CASE WHEN clustering_ordinal_position IS NOT NULL THEN column_name END) AS object_cluster_columns
  FROM `region-us-west1`.INFORMATION_SCHEMA.COLUMNS
  WHERE UPPER(CONCAT(table_schema, '.', table_name)) IN (
    SELECT DISTINCT CONCAT(UPPER(table_schema), '.', UPPER(table_name)) 
    FROM source_classification 
  )
  GROUP BY table_schema, table_name
),
table_size AS (  -- New CTE to get table sizes
  SELECT
    table_schema,
    table_name,
    TOTAL_LOGICAL_BYTES as size_bytes
  FROM `region-us-west1`.INFORMATION_SCHEMA.TABLE_STORAGE
  WHERE UPPER(CONCAT(table_schema, '.', table_name)) IN (
    SELECT DISTINCT source
    FROM source_classification
    WHERE source_type = 'Table'
  )
)
SELECT 
  CONCAT(sc.table_schema, '.', sc.table_name) AS object_name,
  sc.object_type,
  op.object_partition_columns,
  COALESCE(op.object_cluster_columns, 'No clustering columns') AS object_cluster_columns,
  sc.source AS source_object,
  sc.source_type,
  tp.source_partition_columns,
  COALESCE(tp.source_cluster_columns, 'No clustering columns') AS source_cluster_columns,
  ts.size_bytes/1024/1024/1024 AS source_table_size_gb 
FROM source_classification sc
LEFT JOIN table_partitioning tp
  ON UPPER(sc.source) = UPPER(CONCAT(tp.table_schema, '.', tp.table_name))
LEFT JOIN object_partitioning op
  ON UPPER(CONCAT(sc.table_schema, '.', sc.table_name)) = UPPER(CONCAT(op.table_schema, '.', op.table_name))
LEFT JOIN table_size ts  -- Join with the table_size CTE
  ON UPPER(sc.source) = UPPER(CONCAT(ts.table_schema, '.', ts.table_name))
;
