import os
from datetime import datetime, timedelta
from airflow.operators import python_operator
from airflow import DAG
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import Variable
from google.cloud import bigquery
import logging
from airflow import DAG
from airflow.operators.email import EmailOperator
from io import StringIO
import pandas as pd


env = os.environ["instance"]
print(env)

var_input = Variable.get("bq_job_alert_sql")
print(f"var_input: {var_input}")
variable_value_csv= StringIO(var_input)
print(f"variable_value_csv: {variable_value_csv}")
df = pd.read_csv(variable_value_csv)

gcp_conn_id = "dataops_conn_id"
logging.info("gcp_conn_id: " + gcp_conn_id)

#v_project_id = "<PROJECT_ID>" + env + "-prj-01"
v_project_id = "<PROJECT_ID>" + env

bqhook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
bq_client = bqhook.get_client(project_id=v_project_id)


def execute_query():

    bq_query_list = list(df["queries"])
    
    for (query_to_execute) in bq_query_list:

        logging.info("Query to execute: " + str(query_to_execute))

        # Temporary Table Name
        temp_table_id = f'temp_table_{datetime.now().strftime("%Y%m%d%H%M%S")}'
        #temp_table_name = f'{v_project_id}.udco_ds_audit.{temp_table_id}'
        
        #CREATE TEMP TABLE statement
        print(f"query_to_execute: {query_to_execute}")

        full_query = f"""
        CREATE OR REPLACE TEMP TABLE `{temp_table_id}` AS
        {query_to_execute}

        SELECT * FROM `{temp_table_id}`;
        """
        print(f"full_query: {full_query}")

        # Execute the full query
        bq_result = bq_client.query(full_query)
        print(f"bq_result: {bq_result}")
        # Extract and format results as string
        #result_df = bq_result.to_dataframe()
        #print(f"result_df: {result_df}")
        #result_string = result_df.to_html(index=False)
        #print(f"result_string: {result_string}")
        
        #result_df = pd.DataFrame(result_string)
        
        html_content = """
        <html>
        <body>
          <table border="1">
              <tr>
              <th>PROJECT_ID</th>
              <th>JOB_ID</th>
              <th>PARENT_JOB_ID</th>
              <th>START_TIME</th>
              <th>END_TIME</th>
              <th>QUERY</th>
              <th>USER_EMAIL</th>
              <th>TOTAL_BYTES_PROCESSED_TB</th>
            </tr>
        """

        for record in bq_result:
            project_id = record[0]
            job_id = record[1]
            parent_job_id = record[2]
            start_time = record[3]
            end_time = " " if record[4] is None else record[4]
            query = record[5]
            user_email = record[6]
            total_bytes_processed_tb = " " if record[7] is None else record[7]
    
            color = ""
            if total_bytes_processed_tb >= 0.5:
                color = "red"
            elif 0.1 <= total_bytes_processed_tb <= 0.5:
                color = "yellow"
            elif total_bytes_processed_tb <= 0.1:
                color = "green"
            else:
                color= "red"
            
            html_content += """<tr bgcolor="%s">
                    <td>%s</td>
                    <td>%s</td>
                    <td>%s</td>
                    <td>%s</td>
                    <td>%s</td>
                    <td>%s</td>
                    <td>%s</td>
                    <td>%s</td>
                  </tr>""" % (color, project_id, job_id, parent_job_id, start_time, end_time, query, user_email, total_bytes_processed_tb)

        html_content += "</table></body></html>"
                
        send_email_task = EmailOperator(
            task_id="send_email",
            to="prashant111ahire@gmail.com",
            subject=f"BQ Queries executed in the past 2 hours",
            html_content=html_content,
        )
        send_email_task.execute({})

default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    "bq_job_alert_email",
    default_args=default_args,
    max_active_tasks=4,
    max_active_runs=1,
    start_date=datetime(2024, 2, 11),
    schedule_interval="0 * * * *",
    catchup=False,
    is_paused_upon_creation=True,
    tags=["monitoring", "common"],
) as dag:

    extract_email_data = python_operator.PythonOperator(
        task_id="extract_email_data", python_callable=execute_query
    )

    extract_email_data
