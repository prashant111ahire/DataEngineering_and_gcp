##This code will help to extract from BQ using bqhook and compose the extracted files to one file

from google.cloud import storage
import time
from multiprocessing import Process
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery


def export_bq_to_gcs(
    project_id,
    database_name,
    table_name,
    sql,
    gcp_conn_id,
    region,
    GCS_FILE_PATH,
    **kwargs,
):
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
    client = bq_hook.get_client(project_id=project_id, location=region)

    configuration = {
        "query": {
            "query": sql,
            "useLegacySql": False,
        }
    }

    configuration = {
        "query": {
            "query": f"""CREATE OR REPLACE TABLE {project_id}.{database_name}.{table_name} AS {sql} """,
            "useLegacySql": False,
        }
    }
    bq_hook.insert_job(
        configuration=configuration, project_id=project_id, location=region
    )

    destination_uri = GCS_FILE_PATH

    job_config = bigquery.job.ExtractJobConfig(
        destination_format=bigquery.DestinationFormat.CSV,
        field_delimiter="|",
        print_header=False,
    )

    extract_job = client.extract_table(
        f"{project_id}.{database_name}.{table_name}",
        destination_uri,
        job_config=job_config,
        location=region,
    )

    extract_job.result()
    if extract_job.errors:
        raise RuntimeError(f"Export failed: {extract_job.errors}")
    else:
        print(f"Export to {destination_uri} successful!")

    # Drop the temporary table after export
    configuration = {
        "query": {
            "query": f"""DROP TABLE {project_id}.{database_name}.{table_name}""",
            "useLegacySql": False,
        }
    }
    bq_hook.insert_job(
        configuration=configuration, project_id=project_id, location=region
    )


def compose_gcs_files(
    gcp_conn_id: str,
    bucket_name: str,
    source_blobs: list,
    destination_blob_name: str,
    delete_source: bool = True,
):
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    client = gcs_hook.get_conn()
    bucket = client.bucket(bucket_name)
    destination_blob = bucket.blob(destination_blob_name)

    # Compose files
    destination_blob.compose(source_blobs)

    if delete_source:
        for blob in source_blobs:
            try:
                blob.delete()
            except Exception as e:
                time.sleep(1)


def run_parallel_compose(
    gcp_conn_id: str,
    bucket_name: str,
    source_prefix: str,
    final_destination_blob_name: str,
    delete_source: bool = True,
):
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    client = gcs_hook.get_conn()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=source_prefix))
    single_file = blobs[0]

    if len(blobs) < 1:
        print("File Not Extracted from BQ !! ")
    if len(blobs) < 2:
        print("Not enough files to compose. At least two files are required.")
        print("Single Filename is :", single_file.name)

        dest_blob = bucket.blob(final_destination_blob_name)
        dest_blob.rewrite(single_file)
        try:
            # blob = bucket.blob(single_file)
            single_file.delete()
            # blob.delete()
        except Exception as e:
            print(f"Error deleting file {single_file.name}: {e}")
            time.sleep(1)

        return []

    # Determine the number of intermediate blobs dynamically
    num_intermediate_blobs = min(
        len(blobs) // 2, 10
    )  # Create at most 10 intermediate blobs, or half the number of source blobs

    # Extract the directory from final_destination_blob_name to use as intermediate prefix.
    intermediate_prefix = final_destination_blob_name.rsplit("/", 1)[0]

    # Split source blobs into chunks for parallel processing
    chunk_size = (
        len(blobs) + num_intermediate_blobs - 1
    ) // num_intermediate_blobs  # Divide files evenly
    chunks = [blobs[i : i + chunk_size] for i in range(0, len(blobs), chunk_size)]

    intermediate_blob_names = []
    processes = []
    for i, chunk in enumerate(chunks):
        destination_blob_name = f"{intermediate_prefix}/composed_file_{i + 1}.txt"
        intermediate_blob_names.append(destination_blob_name)
        p = Process(
            target=compose_gcs_files,
            args=(
                gcp_conn_id,
                bucket_name,
                chunk,
                destination_blob_name,
                delete_source,
            ),
        )
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    return intermediate_blob_names


def final_compose(
    gcp_conn_id: str,
    bucket_name: str,
    intermediate_blob_names: list,
    final_destination_blob_name: str,
    delete_intermediate: bool = True,
):
    """Compose the final file from the intermediate files and optionally delete intermediate files."""
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    client = gcs_hook.get_conn()
    bucket = client.bucket(bucket_name)
    source_blobs = [bucket.blob(name) for name in intermediate_blob_names]

    print(
        f"Composing {len(source_blobs)} intermediate files into {final_destination_blob_name}..."
    )
    final_blob = bucket.blob(final_destination_blob_name)
    final_blob.compose(source_blobs)
    print(f"Final composition complete: {final_destination_blob_name}")

    if delete_intermediate:
        for blob_name in intermediate_blob_names:
            try:
                blob = bucket.blob(blob_name)
                blob.delete()
            except Exception as e:
                time.sleep(1)  # Prevents rapid deletion errors


def main_gcsfilecompose(
    gcp_conn_id: str,
    bucket_name: str,
    source_prefix: str,
    final_destination_blob_name: str,
    delete_intermediate: bool = True,
):
    # input
    # bucket_name = "test-data-datametica"
    # source_prefix = "item/"
    # final_destination_blob_name = "item_out/item.csv"

    # Step 1: Compose files in parallel
    print("Starting parallel composition of intermediate files...")
    start_time = time.time()
    intermediate_blob_names = run_parallel_compose(
        gcp_conn_id,
        bucket_name,
        source_prefix,
        final_destination_blob_name,
        delete_source=True,
    )
    print(f"Parallel composition completed in {time.time() - start_time:.2f} seconds.")

    # Step 2: Compose the final file from the intermediate files and delete intermediates
    if intermediate_blob_names:
        print("Starting final composition...")
        start_time = time.time()
        final_compose(
            gcp_conn_id,
            bucket_name,
            intermediate_blob_names,
            final_destination_blob_name,
            delete_intermediate=True,
        )
        print(f"Final composition completed in {time.time() - start_time:.2f} seconds.")
    else:
        print("No intermediate files were created. Final composition skipped.")
