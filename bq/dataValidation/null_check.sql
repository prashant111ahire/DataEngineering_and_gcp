-- sql statement that might help you find columns with empty and nulls values :
DECLARE columns ARRAY<STRUCT<name STRING>>;
DECLARE query STRING;
DECLARE total_rows INT64;
DECLARE dataset_name STRING DEFAULT '<PROJECT_NAME>';
DECLARE schema_name STRING DEFAULT '<DATASET_NAME>';
DECLARE table_name STRING DEFAULT '<TABLE_NAME>';
DECLARE full_table_name STRING DEFAULT FORMAT('`%s.%s.%s`', dataset_name, schema_name, table_name);
DECLARE t_table_name STRING DEFAULT FORMAT('`%s.%s.INFORMATION_SCHEMA.COLUMNS`', dataset_name, schema_name);


EXECUTE IMMEDIATE FORMAT('SELECT COUNT(*) FROM %s', full_table_name) INTO total_rows;

EXECUTE IMMEDIATE FORMAT  ( ' SELECT ARRAY_AGG(STRUCT(column_name AS name)) FROM  %s  WHERE LOWER(table_name) = LOWER("%s") AND data_type IN ("STRING", "BYTES")'
, t_table_name, table_name
) INTO columns;

SET query = (Press Alt+F1 for Accessibility Options.

  SELECT
    "SELECT column_name, null_count, empty_count, " || CAST(total_rows AS STRING) || " as total_rows FROM (" || STRING_AGG(
      "SELECT '" || name || "' as column_name, COUNTIF(" || name || " IS NULL) as null_count, COUNTIF(" || name || " = '') as empty_count FROM " || full_table_name || ""
      , ' UNION ALL '
    ) || ")"
  FROM UNNEST(columns)
);

EXECUTE IMMEDIATE query;
