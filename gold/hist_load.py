from apache_beam import Pipeline, ParDo, Create, io, Map
from utils import FetchDataFromAPI, log_data, FlattenJSONData, AddColumnArea, AddInsertDate, get_table_schema, get_arrow_schema
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions
from datetime import datetime
import json



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
    
    # Define API URL and GCS bucket  
    api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
    bucket_name = "earthquake_analysis_data1"
    current_date = datetime.now().strftime('%Y%m%d')
    
    bronze_output_path = f"gs://{bucket_name}/beam/landing/{current_date}/earthquake_raw"
    silver_output_path = f"gs://{bucket_name}/beam/silver/{current_date}/earthquake_transformed"
    parquet_output_path = f"gs://{bucket_name}/beam/silver/{current_date}/earthquake_transformed"
    
    table_spec = 'gcp-data-project-440907:earthquake_ingestion.earthquake_table_dataflow'
    
    with Pipeline(options=options) as p:
        # Fetch data from API and write raw data to GCS
        raw_data = (p
                    | 'Create API URL' >> Create([api_url])
                    | 'Fetch Data from API' >> ParDo(FetchDataFromAPI())
                   )
        
        # Write raw data to GCS
        write_raw_data_to_gcs = (raw_data
                                 | "Format To JSON" >> Map(lambda x: json.dumps(x))
                                 | "Write Raw Data to GCS" >> io.WriteToText(bronze_output_path, 
                                    file_name_suffix=".json", 
                                    num_shards=1
                                    )
                               )
        
        # Read raw data from GCS
        raw_data_from_gcs = (write_raw_data_to_gcs
                             | 'Read Raw Data from GCS' >> io.ReadFromText(bronze_output_path +"*.json")
                             | 'Parse JSON' >> Map(json.loads)
                            )

        # Transform and flatten the data
        transformed_data = (raw_data_from_gcs
                            | 'Flatten JSON Data' >> ParDo(FlattenJSONData())
                           )
        
        # Add 'area' column based on the 'place' value
        transformed_with_area = (transformed_data
                                 | 'Add Column Area' >> ParDo(AddColumnArea())
                                )
        
        # Write transformed data to Parquet before adding insert date
        transformed_with_area | 'Write to Parquet' >> io.WriteToParquet(
            file_path_prefix=parquet_output_path,
            schema=get_arrow_schema(),
            file_name_suffix=".parquet"
        )
        
        # Read the Parquet file back for the insert date operation
        transformed_with_insert_date = (p
                                        | 'Read Transformed Data from Parquet' >> io.ReadFromParquet(parquet_output_path + "*.parquet")
                                        | 'Add Insert Date' >> ParDo(AddInsertDate())
                                        # |'Log Data' >> Map(log_data)
                                       )
        
        
        
        # Write transformed data to BigQuery
        transformed_with_insert_date | 'Write to BigQuery' >> io.WriteToBigQuery(
            table=table_spec,
            schema=get_table_schema(),
            write_disposition=io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=io.BigQueryDisposition.CREATE_IF_NEEDED
        )


if __name__ == "__main__":
    run()