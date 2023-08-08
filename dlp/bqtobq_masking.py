#Code: De-identify Template Test Case
#Purpose: 
#1. This code will mask the CREDIT_CARD_NUMBER with hash(#) keeping special character as it is
#2. Also it will mark INDIA_PAN_INDIVIDUAL with hash(#) keeping special character and first three character(source system) as it is

from typing import List
from google.cloud import dlp
from google.cloud import bigquery
import string
import time

def deidentify_with_mask(
    project: str,
    dataset_id: str,
    input_table_id: str,
    output_table_id: str,
    info_types: List[str],
    masking_character: str = None,
    number_to_mask: int = 0,
) -> None:
    # Instantiate DLP client
    dlp_client = dlp.DlpServiceClient()

    # Convert the project id into a full resource id.
    parent = f"projects/{project}"

    # Construct inspect configuration dictionary
    inspect_config = {"info_types": [{"name": info_type} for info_type in info_types]}

    # Create a BigQuery client
    bq_client = bigquery.Client(project=project)

    # Fetch data from the input BigQuery table
    input_dataset_ref = bq_client.dataset(dataset_id)
    input_table_ref = input_dataset_ref.table(input_table_id)

    rows = bq_client.list_rows(input_table_ref)

    # Create a new BigQuery table with the masked data
    output_dataset_ref = bq_client.dataset(dataset_id)
    output_table_ref = output_dataset_ref.table(output_table_id)

    # Get the schema of the input table
    input_schema = bq_client.get_table(input_table_ref).schema

    # Create a list to store the masked rows
    rows_to_insert = []

    # Process each row and mask sensitive data
    for row in rows:
        masked_row = {}

        # Process each field in the row
        for field in row.keys():
            # Get the field's schema information
            field_schema = next(field_info for field_info in input_schema if field_info.name == field)

            # If the field is a nested field or record, convert it to a JSON string
            if field_schema.field_type == "RECORD":
                item = {"value": str(row[field].to_api_repr())}
            else:
                item = {"value": str(row[field])}

            response = dlp_client.deidentify_content(
                parent=parent,
                deidentify_config={
                    "info_type_transformations": {
                        "transformations": [
                            {
                                "info_types": [{"name": info_type}],
                                "primitive_transformation": {
                                    "character_mask_config": {
                                        "masking_character": masking_character,
                                        "number_to_mask": number_to_mask,
                                        "characters_to_ignore": [
                                            {
                                                "characters_to_skip": string.ascii_letters + string.punctuation
                                            }
                                        ],
                                    }
                                },
                            }
                            for info_type in info_types if info_type != "INDIA_PAN_INDIVIDUAL"
                        ]
                    }
                },
                inspect_config=inspect_config,
                item=item,
            )

            # Custom masking for PAN
            if "INDIA_PAN_INDIVIDUAL" in info_types and field == 'pan':
                original_pan = str(row[field])
                masked_pan = original_pan[:3] + masking_character * (len(original_pan) - 3)
                response.item.value = masked_pan

            # Store the masked value in the corresponding column
            masked_row[field] = response.item.value

        rows_to_insert.append(masked_row)

    # Create the new BigQuery table with the masked data
    schema = [bigquery.SchemaField(field.name, field.field_type) for field in input_schema]
    table = bigquery.Table(output_table_ref, schema=schema)

    # Print information about the table being created
    print("Creating the output table...")
    print(f"Project: {project}")
    print(f"Dataset ID: {dataset_id}")
    print(f"Table ID: {output_table_id}")

    try:
        bq_client.create_table(table)
        print(f"Table '{output_table_id}' created successfully.")
    except Exception as e:
        print(f"Error creating table '{output_table_id}': {e}")

    # Add a sleep delay (5 seconds) before inserting the rows
    print("Sleeping for 5 seconds to allow table creation to propagate...")
    time.sleep(5)

    # Print information about the table being used for insertion
    print(f"Inserting rows into the output table: {output_table_ref.path}")

    # Use the load_table_from_json method to insert rows directly
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    try:
        bq_client.load_table_from_json(
            rows_to_insert,
            output_table_ref,
            job_config=job_config
        )
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting rows: {e}")

# Example usage:
input_dataset_id = "learning_ds"
input_table_id = "dlp_input_tab"
output_dataset_id = "learning_ds"
output_table_id = "dlp_output_tab"
info_types_list = ["CREDIT_CARD_NUMBER"]
mask_character = "#"  # This will be used for other info types except PAN
number_to_mask = 25

deidentify_with_mask(
    project_id,
    input_dataset_id,
    input_table_id,
    output_table_id,
    info_types_list,
    mask_character,
    number_to_mask,
)
