import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, WorkerOptions
from apache_beam.io.parquetio import WriteToParquet, ReadFromParquet
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import os
from utils import (
    FetchDataFromAPI,
    FlattenJSONData,
    AddColumnArea,
    AddInsertDate,
    arrow_schema,
    log_data
)
import json
from datetime import datetime

# Set up your Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Dinesh Mote\Downloads\gcp-data-project-440907-eb61e9727efa.json"

def run():
    # Set up Beam pipeline options
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'gcp-data-project-440907'
    google_cloud_options.job_name = 'api-data-to-gcs'
    google_cloud_options.region = "us-east4"
    google_cloud_options.temp_location = 'gs://earthquake_analysis_data1/stage_loc'
    google_cloud_options.staging_location = 'gs://earthquake_analysis_data1/temp_loc'
    
    worker_options = options.view_as(WorkerOptions)
    worker_options.machine_type = 'n1-standard-4'
    
    # Define API URL and GCS paths
    api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
    bucket_name = "earthquake_analysis_data1"
    current_date = datetime.now().strftime('%Y%m%d')
    
    bronze_output_path = f"gs://{bucket_name}/beam/daily_landing/{current_date}/earthquake_raw"
    parquet_output_path = f"gs://{bucket_name}/beam/silver_daily_load/{current_date}/earthquake_transformed"
    hist_table_spec = 'gcp-data-project-440907:earthquake_ingestion.earthquake_table_dataflow'
    # daily_table_spec = 'gcp-data-project-440907:earthquake_ingestion.daily_earthquake_table_dataflow'

    # Define BigQuery table schema
    table_schema = {
        "fields": [
            {"name": "place", "type": "STRING", "mode": "NULLABLE"},
            {"name": "mag", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "tz", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "detail", "type": "STRING", "mode": "NULLABLE"},
            {"name": "felt", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "cdi", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "mmi", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "alert", "type": "STRING", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tsunami", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "sig", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "net", "type": "STRING", "mode": "NULLABLE"},
            {"name": "code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ids", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sources", "type": "STRING", "mode": "NULLABLE"},
            {"name": "types", "type": "STRING", "mode": "NULLABLE"},
            {"name": "nst", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "dmin", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rms", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "gap", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "magType", "type": "STRING", "mode": "NULLABLE"},
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "geometry", "type": "RECORD", "mode": "NULLABLE", "fields": [
                {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "depth", "type": "FLOAT", "mode": "NULLABLE"}
            ]},
            {"name": "area", "type": "STRING", "mode": "NULLABLE"},
            {"name": "insert_date", "type": "DATE", "mode": "NULLABLE"}
        ]
    }
    
    with beam.Pipeline(options=options) as p:
        # Fetch data from API
        raw_data = (p | 'Create API URL' >> beam.Create([api_url])
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
                              table=hist_table_spec, schema=table_schema,
                              write_disposition=BigQueryDisposition.WRITE_APPEND,
                              create_disposition=BigQueryDisposition.CREATE_IF_NEEDED))

if __name__ == "__main__":
    run()
