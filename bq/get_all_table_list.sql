--Purpose: This script will help to all the list of the table from a project from all datasets

DECLARE sql,dataset_name,PROJECT_ID,table_name STRING;

-- Create a temporary table to store the results
CREATE TEMPORARY TABLE all_tables (
  dataset_name STRING,
  table_name STRING
);

SET PROJECT_ID="<your-project-id>";

-- Get all dataset names in the project
FOR dataset IN (
  SELECT schema_name
  FROM `your-project-id`.INFORMATION_SCHEMA.SCHEMATA
  WHERE schema_name NOT LIKE '%_schema'
)
DO
  SET dataset_name = dataset.schema_name;
  
  -- Generate SQL dynamically to get all table names in each dataset
  SET sql = (
    SELECT CONCAT('INSERT INTO `<your-project-id>.<your-dataset>.all_tables` SELECT ','"',PROJECT_ID,'","', dataset_name, '" AS dataset_name, table_name FROM `', dataset_name, '.INFORMATION_SCHEMA.TABLES` WHERE table_type = "BASE TABLE"')
  );

  -- Execute the dynamic SQL and insert the results into the temporary table
  EXECUTE IMMEDIATE sql;

END FOR;
