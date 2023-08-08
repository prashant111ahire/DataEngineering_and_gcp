#Purpose: This code will help to encrypt the data using DLP will keep the length of the input data same after encryption

import logging
from typing import Dict, List, Union
from google.cloud import bigquery
import google.cloud.dlp_v2
import base64

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def deidentify_table_replace_with_info_types(
    project: str,
    table_data: Dict[str, Union[List[bytes], List[List[bytes]]]],
    info_types: List[str],
    deid_content_list: List[str],
) -> List[List[str]]:

    
    
    # Instantiate a client.
    dlp = google.cloud.dlp_v2.DlpServiceClient()

    # Construct the `table`.
    headers = [{"name": val} for val in table_data["header"]]
    rows = []
    for row in table_data["rows"]:
        rows.append({"values": [{"string_value": cell_val.decode("utf-8")} for cell_val in row]})

    table = {"headers": headers, "rows": rows}

    # Construct item
    item = {"table": table}

    # Specify fields to be de-identified
    deid_content_list = [{"name": _i} for _i in deid_content_list]

     # Construct FPE configuration dictionary
    crypto_replace_ffx_fpe_config = {
        "crypto_key": {
            "kms_wrapped": {"wrapped_key": wrapped_key, "crypto_key_name": kms_key_name}
        },
        "custom_alphabet": alphabet
    }

    # Construct inspect configuration dictionary
    inspect_config = {"info_types": [{"name": info_type} for info_type in info_types]}

    # Construct deidentify configuration dictionary
    deidentify_config = {
        "record_transformations": {
            "field_transformations": [
                {
                    "primitive_transformation": {
                        "crypto_replace_ffx_fpe_config": crypto_replace_ffx_fpe_config
                            },
                            "fields": deid_content_list,
                            }
                    ]
                }
            }

    # Convert the project id into a full resource id.
    parent = f"projects/{project}"

    # Call the API.
    response = dlp.deidentify_content(
        request={
            "parent": parent,
            "deidentify_config": deidentify_config,
            "item": item,
            "inspect_config": inspect_config,
        }
    )

    # Modify the way you access the de-identified table data.
    # The response will contain an `item` field, which should be accessed as follows:
    deid_rows = [
        [
            cell_val.string_value
            for cell_val in row.values
        ]
        for row in response.item.table.rows
    ]

    return deid_rows

def deidentify_bigquery_table_replace_with_info_types(
    project: str,
    dataset_id: str,
    table_id: str,
    info_types: List[str],
    deid_content_list: List[str],
    output_dataset_id: str,
    output_table_id: str,
    kms_key_name: str,
    wrapped_key: str,
) -> None:
    bq_client = bigquery.Client(project=project)

    # Fetch the table schema to get column names and types.
    table_ref = f"{project}.{dataset_id}.{table_id}"
    table = bq_client.get_table(table_ref)

    # Get column names and types from the schema.
    column_names = [field.name for field in table.schema]
    column_types = [field.field_type for field in table.schema]

    # Construct the SQL query to fetch data from the specified table and columns.
    columns_string = ", ".join(column_names)
    query = f"SELECT {columns_string} FROM `{table_ref}`"

    # Run the query to fetch the data from BigQuery.
    query_job = bq_client.query(query)
    rows = query_job.result()

    # Convert the query result into the table_data format used in the original function.
    table_data = {"header": column_names, "rows": [list(row) for row in rows]}

    # Convert data to bytes
    table_data_bytes = {
        "header": [col.encode("utf-8") for col in table_data["header"]],
        "rows": [[str(val).encode("utf-8") if val is not None else b"" for val in row] for row in table_data["rows"]],
    }

    # Use the deidentify_table_replace_with_info_types function to get the de-identified data.
    logger.info("De-identifying table data...")
    deid_data = deidentify_table_replace_with_info_types(
        project=project,
        table_data=table_data_bytes,
        info_types=info_types,
        deid_content_list=deid_content_list,
    )
    logger.info("Table data de-identified successfully.")

    # Prepare the de-identified data for insertion into the output table.
    output_rows = [dict(zip(column_names, row)) for row in deid_data]

    # Create the output BigQuery table if it doesn't exist.
    output_dataset_ref = bq_client.dataset(output_dataset_id)
    output_table_ref = output_dataset_ref.table(output_table_id)

    try:
        bq_client.get_table(output_table_ref)
    except Exception:
        # Create output schema with the same column names and types as the input table,
        # but change the data type of the de-identified field to STRING.
        output_schema = [
            bigquery.SchemaField(name=col_name, field_type="STRING" if col_name in deid_content_list else col_type)
            for col_name, col_type in zip(column_names, column_types)
        ]
        output_table = bigquery.Table(output_table_ref, schema=output_schema)
        bq_client.create_table(output_table)

    # Insert the de-identified data into the output table.
    logger.info("Inserting de-identified data into the output table...")
    bq_client.insert_rows_json(output_table_ref, output_rows)
    logger.info("Data inserted successfully.")

# Example usage:
project_id = "your-project-id"
dataset_id = "input-your-dataset"
table_id = "input-table"
output_dataset_id = "ouput-your-dataset"  # Specify the output dataset ID
output_table_id = "output-table"  # Specify the output table ID
info_types_list = ["CREDIT_CARD_NUMBER"]  # Add more info types if needed
deid_content_list = ["credit_card_num"]  # Add more fields to de-identify if needed
kms_key_name = "projects/<your-project-id>/locations/global/keyRings/<key-ring-name>/cryptoKeys/<key-name>"
kms_wrapped_key_name_base64 = "<wrapped-key>"
alphabet = """0123456789-"""  # Define the custom alphabet(This will be the DLP data which we need to encrypt,it will be consist of below characters)

deidentify_bigquery_table_replace_with_info_types(
    project=project_id,
    dataset_id=dataset_id,
    table_id=table_id,
    info_types=info_types_list,
    deid_content_list=deid_content_list,
    output_dataset_id=output_dataset_id,
    output_table_id=output_table_id,
    kms_key_name=kms_key_name,
    wrapped_key=wrapped_key,
)
