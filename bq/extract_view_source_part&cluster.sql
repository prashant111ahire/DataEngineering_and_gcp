--This will extract source of the views with partition and clustering details

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
  FROM region-us-west1.INFORMATION_SCHEMA.TABLES
  WHERE UPPER(CONCAT(table_schema, '.', table_name)) IN (
    'DEV_NAP_BASE_VWS.MERCH_INVENTORY_SKU_STORE_DAY_COLUMNAR_VW',
    'DEV_NAP_BASE_VWS.MERCH_INVENTORY_SKU_STORE_DAY_FACT',
    'DEV_NAP_BASE_VWS.STORE_DIM',
    'DEV_NAP_BASE_VWS.WEIGHTED_AVERAGE_COST_DATE_DIM',
    'DEV_NAP_BASE_VWS.WEIGHTED_AVERAGE_COST_CHANNEL_DIM',
    'DEV_NAP_BASE_VWS.PRICE_STORE_DIM_VW',
    'DEV_NAP_BASE_VWS.PRODUCT_PRICE_TIMELINE_DIM',
    'DEV_NAP_BASE_VWS.DAY_CAL_454_DIM',
    'DEV_NAP_BASE_VWS.CONFIG_LKUP',
    'ONEHOP_ETL_APP_DB.APP04070_SOURCE_KAFKA_CONSUMER_OFFSET_BATCH_DETAILS',
    'DEV_NAP_STG.PRODUCT_WEB_STYLE_REVIEW_DIM_VTW_TMP',
    'DEV_NAP_STG.PRODUCT_WEB_STYLE_REVIEW_DIM_VTW_TMP'
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
  LEFT JOIN region-us-west1.INFORMATION_SCHEMA.VIEWS v
    ON UPPER(bs.source) = UPPER(CONCAT(v.table_schema, '.', v.table_name))
),
table_partitioning AS (
  SELECT 
    table_schema,
    table_name,
    STRING_AGG(CASE WHEN is_partitioning_column = 'YES' THEN column_name END) AS source_partition_columns,
    STRING_AGG(CASE WHEN clustering_ordinal_position IS NOT NULL THEN column_name END) AS source_cluster_columns
  FROM region-us-west1.INFORMATION_SCHEMA.COLUMNS
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
  FROM region-us-west1.INFORMATION_SCHEMA.COLUMNS
  WHERE UPPER(CONCAT(table_schema, '.', table_name)) IN (
    SELECT DISTINCT CONCAT(UPPER(table_schema), '.', UPPER(table_name)) 
    FROM source_classification 
  )
  GROUP BY table_schema, table_name
)
SELECT 
  CONCAT(sc.table_schema, '.', sc.table_name) AS object_name,
  sc.object_type,
  op.object_partition_columns,
  COALESCE(op.object_cluster_columns, 'No clustering columns') AS object_cluster_columns,
  sc.source AS source_object,
  sc.source_type,
  tp.source_partition_columns,
  COALESCE(tp.source_cluster_columns, 'No clustering columns') AS source_cluster_columns
FROM source_classification sc
LEFT JOIN table_partitioning tp
  ON UPPER(sc.source) = UPPER(CONCAT(tp.table_schema, '.', tp.table_name))
LEFT JOIN object_partitioning op
  ON UPPER(CONCAT(sc.table_schema, '.', sc.table_name)) = UPPER(CONCAT(op.table_schema, '.', op.table_name));

------------------------------------------------
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
  FROM `region-us-west1`.INFORMATION_SCHEMA.TABLES  -- Added backticks for consistency
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
  LEFT JOIN `region-us-west1`.INFORMATION_SCHEMA.VIEWS v  -- Added backticks
    ON UPPER(bs.source) = UPPER(CONCAT(v.table_schema, '.', v.table_name))
),
table_partitioning AS (
  SELECT 
    table_schema,
    table_name,
    STRING_AGG(CASE WHEN is_partitioning_column = 'YES' THEN column_name END) AS source_partition_columns,
    STRING_AGG(CASE WHEN clustering_ordinal_position IS NOT NULL THEN column_name END) AS source_cluster_columns
  FROM `region-us-west1`.INFORMATION_SCHEMA.COLUMNS  -- Added backticks
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
  FROM `region-us-west1`.INFORMATION_SCHEMA.COLUMNS -- Added backticks
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
  FROM `region-us-west1`.INFORMATION_SCHEMA.TABLE_STORAGE  -- Use TABLE_STORAGE for size
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
  ts.size_bytes/1024/1024/1024 AS source_table_size_gb  -- Added the size information
FROM source_classification sc
LEFT JOIN table_partitioning tp
  ON UPPER(sc.source) = UPPER(CONCAT(tp.table_schema, '.', tp.table_name))
LEFT JOIN object_partitioning op
  ON UPPER(CONCAT(sc.table_schema, '.', sc.table_name)) = UPPER(CONCAT(op.table_schema, '.', op.table_name))
LEFT JOIN table_size ts  -- Join with the table_size CTE
  ON UPPER(sc.source) = UPPER(CONCAT(ts.table_schema, '.', ts.table_name))
;
