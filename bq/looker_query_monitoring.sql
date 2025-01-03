--FOR CREATING LOOKER DASHBOARD FOR MONITORING
--GET RESOURCE DETAILS FOR BIGQUERY PROJECT 
SELECT
  project_id,
  job_id,
  parent_job_id,
  user_email,
  creation_time,
  start_time,
  end_time,
  job_type,
  IFNULL(env_value.value ,'UNASSIGNED') env,   
  IFNULL(dts_app_value.value ,'UNASSIGNED') dts_app,  
  IFNULL(dts_team_value.value ,'UNASSIGNED') dts_team,  
  IFNULL(task_value.value ,'UNASSIGNED') task,
  IFNULL(spname_value.value ,'UNASSIGNED') spname,
  IFNULL(framework_value.value ,'UNASSIGNED') framework,
  labels,
  statement_type,
  state,
  ROUND(total_slot_ms) total_slot_ms,
  ROUND(total_bytes_processed) total_bytes_processed,
  ROUND(total_bytes_billed) total_bytes_billed,
  query ,
  reservation_id,  
  SAFE_DIVIDE(total_slot_ms,TIMESTAMP_DIFF(ifnull(end_time, CURRENT_TIMESTAMP()),start_time,MILLISECOND)) as slots,
  total_bytes_processed/1e9 AS gbs_processed,
  dml_statistics,
  referenced_tables,
  error_result
FROM
PROJECT_ID.ETL_AUDIT.INFORMATION_SCHEMA_JOBS
  left outer join unnest(labels) env_value on env_value.key in ('env' , 'enviornment')
   left outer join  unnest(labels) dts_team_value on dts_team_value.key in('dts-team')
left outer join  unnest(labels) dts_app_value on dts_app_value.key in('dts-app')
  left outer join  unnest(labels) task_value on task_value.key in ('task')
  left outer join  unnest(labels) spname_value on spname_value.key='spname'
  left outer join  unnest(labels) framework_value on framework_value.key='framework'
where statement_type <> 'SCRIPT'


----------==
--Hr utilization matric:  
SUM(total_slot_ms) / (1000 * 60 * 60 )
creation time : MM/d/yy,hh

--Query processing cost 
(total_bytes_billed/1024/1024/1024/1024)*6.25


------------------------------------------------------------------------------------------
--FOR GETTING DETAILS OF LABELS APPLIED OR NOT
select    
  table_catalog,
  table_schema,
  table_name, 
  creation_time,
  labels,
  team.value as dts_team,
  appname.value as dts_app,
  spname.value as spaname,
  env.value as env,
  task.value as task,
  from (
SELECT
    t1.table_catalog,
    t1.table_schema,
    t1.table_name,
    creation_time,
	labels,
    options
FROM (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    creation_time,
    CASE
      WHEN INSTR(ddl, 'labels=')>0 THEN REPLACE(SUBSTR(ddl,INSTR(ddl, 'labels=')+7),');','')
    ELSE
    'NO LABELS'
  END
    AS labels
  FROM
    `PROJECT_ID.region-us.INFORMATION_SCHEMA.TABLES`
  where table_schema not in ('pelican_temp')
) AS t1
LEFT OUTER JOIN (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    ARRAY(
    SELECT
      AS STRUCT arr[
    OFFSET
      (0)] KEY, arr[
    OFFSET
      (1)] value
    FROM
      UNNEST(REGEXP_EXTRACT_ALL(option_value, r'STRUCT\(("[^"]+", "[^"]+")\)')) kv, UNNEST([STRUCT(SPLIT(REPLACE(kv, '"', ''), ', ') AS arr)]) ) OPTIONS
  FROM
    `PROJECT_ID.region-us.INFORMATION_SCHEMA.TABLE_OPTIONS`
  WHERE
    option_name = 'labels') AS t2
ON
  t1.table_schema=t2.table_schema
  AND t1.table_name =t2.table_name
  AND t1.table_catalog=t2.table_catalog)
  left outer join unnest(options) team on team.key in ('dts-team')
  left outer join  unnest(options) appname on appname.key in ('dts-app')
  left outer join  unnest(options) spname on spname.key in ('spname')
  left outer join  unnest(options) env on env.key in ('env')
  left outer join  unnest(options) task on task.key in ('task')

