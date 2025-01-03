--Get clustering/partition with size/row count

select 
sz.dataset_id,
sz.table_id,
sz.row_count,
(sz.size_bytes/1024/1024/1024) size_gb,
obj.partiton_col,
obj.cluster_col
from <DATASET_NAME>.__TABLES__ sz
left join 
(select 
table_schema,
table_name,
max(CASE WHEN is_partitioning_column='YES' then column_name else NULL end) partiton_col,
STRING_AGG(CASE WHEN clustering_ordinal_position is NOT NULL then column_name else null end,',') as cluster_col
from `<DATASET_NAME>.INFORMATION_SCHEMA.COLUMNS`
GROUP BY 1,2
)obj
on sz.dataset_id=obj.table_schema
AND sz.table_id=obj.table_name
order by size_bytes desc


=======================================================================================================================================================

--get source table from view
 select table_schema,table_name,ddl,
 replace(replace(REGEXP_EXTRACT(ddl, r'(?i)(?:from)\s+(.*;)'),"`",""),';','')
from region-us.INFORMATION_SCHEMA.TABLES where table_name='D1_UPC';

=======================================================================================================================================================
--QUERY to get most used table

with referenced_table_list as (
SELECT
a.project_id,
job_id,
creation_time,
referenced_tables.dataset_id,
referenced_tables.table_id
FROM region-us.INFORMATION_SCHEMA.JOBS a
left outer join unnest(referenced_tables) referenced_tables
WHERE job_type = 'QUERY' 
AND DATE(creation_time) >= DATE_SUB(DATE(CURRENT_TIMESTAMP()), INTERVAL 30 DAY)
and a.project_id='<PROJECT_ID>'
)
SELECT
-- project_id,
dataset_id,
table_id,
COUNT(DISTINCT job_id) AS total_jobs
FROM referenced_table_list
GROUP BY 1,2
ORDER BY 3 DESC

=======================================================================================================================================================

--Get clustering/partition with size/row count FOR DATASETS

CREATE OR REPLACE PROCEDURE `<PROJECT_ID>.<DATASET_NAME>.SP_GET_TABLE_DETAILS`()
BEGIN
DECLARE sql,dataset_name,PROJECT_ID,table_name STRING;

SET @@query_label="appcode:uddi,domain_name:dataops,env:prod,spname:sp_get_all_details";

--TABLE_STORGE COPY FROM INFORMATION_SCHEMA
CREATE or REPLACE TABLE <DATASET_NAME>.TABLE_STORAGE cluster by table_schema,table_name AS
SELECT
  table_schema,
  table_name,
  total_rows,
  total_logical_bytes,
  total_partitions
FROM
  region-us.INFORMATION_SCHEMA.TABLE_STORAGE;

--ADD EXPIRATION FOR 1 DAY
ALTER TABLE <DATASET_NAME>.TABLE_STORAGE
SET OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) -- Replace 1 with your desired expiration period in days
);

--THE FINAL TABLE
CREATE or replace table `<DATASET_NAME>.ALL_TABLE_DETAILS` (
DATASET_NAME STRING,
TABLE_NAME STRING,
ROW_COUNT INT64,
SIZE_GB FLOAT64,
PARTITION_COL STRING,
PART_DATATYPE STRING,
TOTAL_PARTITIONS INT64,
CLUSTER_COL STRING,
CLUSTER_DATATYPE STRING,
APPCODE STRING,
BODNAME STRING,
DOMAINNAME STRING,
ENVIRONMENT STRING,
COMMENTS STRING,
)
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) -- Replace 1 with your desired expiration period in days
);

-- Generate SQL dynamically to get all table names in each dataset
  SET sql = (
    SELECT CONCAT('INSERT INTO `<DATASET_NAME>.ALL_TABLE_DETAILS` ',
'WITH SizeInfo AS ',
'( ',
'SELECT tab.table_schema, tab.table_name, ',
'MAX(CASE WHEN col.is_partitioning_column = "YES" THEN col.column_name ELSE NULL END) AS partition_col, ',
'MAX(CASE WHEN col.is_partitioning_column = "YES" THEN col.data_type ELSE NULL END) AS part_datatype, ',
'STRING_AGG(CASE WHEN col.clustering_ordinal_position IS NOT NULL THEN col.column_name ELSE NULL END, ",") AS cluster_col, ',
'STRING_AGG(CASE WHEN col.clustering_ordinal_position IS NOT NULL THEN col.data_type ELSE NULL END, ",") AS cluster_datatype ',
'FROM region-us.INFORMATION_SCHEMA.TABLES tab  ',
'INNER JOIN region-us.INFORMATION_SCHEMA.COLUMNS col  ',
'ON col.TABLE_SCHEMA=tab.TABLE_SCHEMA AND  ',
'col.TABLE_NAME=tab.TABLE_NAME  ',
'WHERE tab.TABLE_TYPE="BASE TABLE"  ',
'GROUP BY 1, 2 ',
'), ',
'TempTable AS ( ',
'select obj.table_schema,obj.table_name,sz.total_rows as row_count,(sz.total_logical_bytes/1024/1024/1024) as SIZE_GB,partition_col,part_datatype,sz.total_partitions, cluster_col,cluster_datatype ,  ',
'from SizeInfo obj ',
'LEFT JOIN  ',
'DATASET_NAME.TABLE_STORAGE sz ',
'ON obj.table_schema=sz.table_schema and obj.table_name=sz.table_name ',
') ,  ',
'LABELS AS ( ',
' SELECT  ',
'table_catalog, table_schema, table_name, appcode, bodname, domainname, environment, ',
'CASE WHEN CASE WHEN appcode = "uddi" THEN 0 ELSE 1 END + CASE WHEN bodname != LOWER(bodname) THEN 1 ELSE 0 END  + ',
'CASE WHEN LENGTH(domainname) > 4 THEN 1 ELSE 0 END + ',
'CASE WHEN environment NOT IN ("prod") THEN 1 ELSE 0 END > 0 THEN "Incorrect Label" ELSE "Correct Label" END  ',
'comments  ',
'FROM ( ',
'SELECT  ',
'table_catalog, table_schema, table_name, option_type, option_value, ',
'CASE  WHEN REGEXP_EXTRACT(option_value, r',"'",'STRUCT\\("appcode", "([^"]+)"',"'",') IS NOT NULL THEN REGEXP_EXTRACT(option_value, r',"'",'STRUCT\\("appcode", "([^"]+)"',"'",')  ELSE "" END AS appcode,',
'CASE  WHEN REGEXP_EXTRACT(option_value, r',"'",'STRUCT\\("bodname", "([^"]+)"',"'",') IS NOT NULL THEN REGEXP_EXTRACT(option_value, r',"'",'STRUCT\\("bodname", "([^"]+)"',"'",')  ELSE "" END AS bodname,',
'CASE  WHEN REGEXP_EXTRACT(option_value, r',"'",'STRUCT\\("domainname", "([^"]+)"',"'",') IS NOT NULL THEN REGEXP_EXTRACT(option_value, r',"'",'STRUCT\\("domainname", "([^"]+)"',"'",')  ELSE "" END AS domainname,',
'CASE  WHEN REGEXP_EXTRACT(option_value, r',"'",'STRUCT\\("environment", "([^"]+)"',"'",') IS NOT NULL THEN REGEXP_EXTRACT(option_value, r',"'",'STRUCT\\("environment", "([^"]+)"',"'",')  ELSE "" END AS environment,',
' FROM `region-us`.INFORMATION_SCHEMA.TABLE_OPTIONS AS t ',
' WHERE option_name = "labels" ) ',
')SELECT part.*, ',
'CASE WHEN lab.appcode="" then null else lab.appcode end appcode, ',
'CASE WHEN lab.bodname="" then null else lab.bodname end bodname, ',
'CASE WHEN lab.domainname="" then null else lab.domainname end domainname, ',
'CASE WHEN lab.environment="" then null else lab.environment end environment, ',
'comments  ',
'FROM TempTable part LEFT JOIN LABELS lab  ',
'ON PART.table_schema=lab.TABLE_SCHEMA AND part.table_name=lab.TABLE_NAME '
));

  -- Execute the dynamic SQL
  EXECUTE IMMEDIATE sql;
END;


==========================================================================================================================
--How to get labels with tables--
select table_schema,table_name, REGEXP_EXTRACT(option_value, r'"appcode",\s*"([^"]+)"') AS appcode,
  REGEXP_EXTRACT(option_value, r'"environment",\s*"([^"]+)"') AS environment,
  REGEXP_EXTRACT(option_value, r'"domainname",\s*"([^"]+)"') AS domainname,
  REGEXP_EXTRACT(option_value, r'"bodname",\s*"([^"]+)"') AS bodname
   from <DATASET_NAME>.INFORMATION_SCHEMA.TABLE_OPTIONS  where option_name='labels'
   
==========================================================================================================================
--How to get labels with stored PROCEDURE--
select 
routine_schema,n
routine_name, 
SPLIT(REGEXP_EXTRACT(ddl, r'@@query_label="([^"]*)"'), ',') AS key_value_pairs,
from <DATASET_NAME>.INFORMATION_SCHEMA.ROUTINES 

SELECT
  routine_schema,
  routine_name,
  IF(
    EXISTS (
      SELECT
        1
      FROM
        UNNEST(SPLIT(REGEXP_EXTRACT(ddl, r'@@query_label="([^"]*)"'), ',')) AS key_value
      WHERE
        key_value LIKE '%appcode:uddi%' or key_value LIKE '%bodname:%' or  key_value LIKE '%domainname:%' or key_value LIKE '%environment:%' or key_value LIKE '%spname:%' or key_value LIKE '%parent_spname:%'
    ),
    'Correct labels',
    'Incorrect labels'
  ) AS appcode_validation,
  SPLIT(REGEXP_EXTRACT(ddl, r'@@query_label="([^"]*)"'), ',') AS key_value_pairs
FROM
  <DATASET_NAME>.INFORMATION_SCHEMA.ROUTINES;


==========================================================================================================================

--How to get size of all the tables--
select
table_id as TABLE_NAME,
size_bytes/1024/1024 as size_in_MB,
size_bytes/1024/1024/1024 as size_in_GB,
size_bytes/1024/1024/1024/1024 as size_in_TB,
from DATASET_NAME.__TABLES__
order by size_bytes desc

==========================================================================================================================

--How to get last modified and creation time for all the tables--

select
table_id as TABLE_NAME,
timestamp_millis(creation_time) as creation_time,
timestamp_millis(last_modified_time) as last_modified_time,
from DATASET_NAME.__TABLES__
--where table_id like '%%'
--where table_id in ('COMMA_SEPERATED_TABLE_LIST')

==========================================================================================================================
--How to get partition expiration days for all tables--

select * from `PROJECT_ID.DATASET_NAME.INFORMATION_SCHEMA.TABLE_OPTIONS`
where option_name='partition_expiration_days'

==========================================================================================================================

--ALL tables columns description
select * from `PROJECT_ID.DATASET_NAME.INFORMATION_SCHEMA.TABLE_OPTIONS`
where option_name='description'

==========================================================================================================================
--How to partition column for all tables
SELECT * 
FROM `PROJECT_ID.DATASET_NAME.INFORMATION_SCHEMA.COLUMNS`
WHERE TABLE_NAME = Table_Name 
AND is_partitioning_column = "YES"


==========================================================================================================================
--Partition wise COUNT

select table_schema,table_name,partition_id,total_rows as count,total_logical_bytes/1024/1024 as size_in_MB, last_modified_time from `cio-datahub-enterprise-pr-183a.src_fwds.INFORMATION_SCHEMA.PARTITIONS`
where table_name in ()

==========================================================================================================================
--How to get table wise count--

select
table_id as TABLE_NAME,
row_count
from DATASET_NAME.__TABLES__
where IN ('COMMA_SEPERATED_TABLE_LIST')
order by row_count desc

==========================================================================================================================

--Get DDL of the tables--
select table_name,ddl from ent_cust_cust.INFORMATION_SCHEMA.TABLES

