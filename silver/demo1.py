import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, WorkerOptions
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import os
import json
from utils import (
    FetchDataFromAPI,
    FlattenJSONData,
    AddColumnArea,
    AddInsertDate,
    arrow_schema,
    log_data
)
from datetime import datetime

# Load configuration
with open(r'C:\Brainworks\earthquake_ingestion_dataflow\silver\config.json') as config_file:
    config = json.load(config_file)

# Set up your Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Dinesh Mote\Downloads\gcp-data-project-440907-eb61e9727efa.json"

def run():
    # Set up Beam pipeline options
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = config["gcp_project"]
    google_cloud_options.job_name = 'api-data-to-gcs'
    google_cloud_options.region = config["region"]
    google_cloud_options.temp_location = config["temp_location"]
    google_cloud_options.staging_location = config["staging_location"]
    
    worker_options = options.view_as(WorkerOptions)
    worker_options.machine_type = config["machine_type"]
    
    # Generate paths and table spec from config
    current_date = datetime.now().strftime('%Y%m%d')
    bronze_output_path = config["bronze_output_path_template"].format(
        bucket_name=config["bucket_name"], current_date=current_date)
    parquet_output_path = config["parquet_output_path_template"].format(
        bucket_name=config["bucket_name"], current_date=current_date)
    table_spec = config["table_spec"]
    table_schema = config["table_schema"]

    with beam.Pipeline(options=options) as p:
        # Fetch data from API
        raw_data = (p | 'Create API URL' >> beam.Create([config["api_url"]])
                      | 'Fetch Data from API' >> beam.ParDo(FetchDataFromAPI()))
        
        # Write raw data to GCS
        raw_data | "Write Raw Data to GCS" >> beam.io.WriteToText(
            bronze_output_path, file_name_suffix=".json", num_shards=1)
        
        # Read and transform data
        transformed_data = (raw_data | 'Flatten JSON Data' >> beam.ParDo(FlattenJSONData())
                                        | 'Add Column Area' >> beam.ParDo(AddColumnArea()))
        
        # Write transformed data to Parquet
        transformed_data | 'Write to Parquet' >> WriteToParquet(
            file_path_prefix=parquet_output_path, schema=arrow_schema, file_name_suffix=".parquet")
        
        # Add insert date and write to BigQuery
        (transformed_data | 'Add Insert Date' >> beam.ParDo(AddInsertDate())
                          | 'Write to BigQuery' >> WriteToBigQuery(
                              table=table_spec, schema=table_schema,
                              write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                              create_disposition=BigQueryDisposition.CREATE_IF_NEEDED))

if __name__ == "__main__":
    run()
