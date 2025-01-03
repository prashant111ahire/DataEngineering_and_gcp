DECLARE datasets ARRAY<STRING>;
DECLARE sql_command STRING;

SET datasets = ARRAY(
  SELECT schema_name
  FROM `region-us-west1.INFORMATION_SCHEMA.SCHEMATA`
  WHERE schema_name NOT IN ('INFORMATION_SCHEMA')
);

FOR dataset_name_i IN (
  SELECT schema_name FROM UNNEST(datasets) AS schema_name
) DO

  SET sql_command = FORMAT("""
    GRANT `roles/bigquery.dataEditor`
    ON SCHEMA `<PROJECT_ID>.%s`
    TO "serviceAccount:<SA>.iam.gserviceaccount.com"
  """, dataset_name_i.schema_name);

  SELECT sql_command;

  EXECUTE IMMEDIATE sql_command;

END FOR;
arts_apps.rp_performance_sku_lw
