import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime

#This is schema for table/pubsub data
input_schema = "name:STRING,email:STRING,address:STRING,phone_number:STRING,job_title:STRING,birthdate:DATE"

def parse_pubsub_msg(element,schema):
    try:
        message_data = json.loads(element.decode('utf-8'))
    except json.JSONDecodeError as e:
        logging.error(f"Error Decoding JSON message: {e}")
        return None
    schema_pairs = schema.split(',')
    parsed_data = {}
	
	#creating dictonary of key and data from pubsub
    for schema_pair in schema_pairs:
        attribute, data_type = schema_pair.split(':')
        attribute = attribute.strip()
        data_type = data_type.strip()
        parsed_data[attribute] = message_data.get(attribute)

    print(parsed_data)
    return parsed_data

def main():
    logging.getLogger().setLevel(logging.INFO)
    
    project_id = "YOUR-PROJECT-ID"
    dataset_id = "YOUR-DATASET"
    table_id = "YOUR-TABLE-NAME"
    subscription = "projects/<YOUR-PROJECT-ID>/subscriptions/<YOUR-SUBSCRIPTION-NAME>"
    date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_time_job = date_time.replace(":","-").replace(" ","-")
	
	#SETTING DATAFLOW ENVIRONMENT
    options = PipelineOptions([
        "--project={}".format(project_id),
        "--runner=DataflowRunner",
        "--job_name=pubsub-to-bq-{}".format(date_time_job),
        "--temp_location=gs://<GCS-BUCKET>",
        "--region=<YOUR-REGION>",
        "--streaming" #FOR STREAMING EXECUTION NEED TO SET
    ])
    
    with beam.Pipeline(options=options) as p:
        data = (
            p
            | "Reading from pub sub" >> beam.io.ReadFromPubSub(subscription=subscription)
            | "Parsing pubsub data" >> beam.Map(parse_pubsub_msg,schema=input_schema)
        )
		
        dataset_rec = f"{dataset_id}.{table_id}"
        table_rec = f"{project_id}:{dataset_rec}"
    
        data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table=table_rec,
            schema=input_schema,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND"
        )
        
        result = p.run()
        result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()