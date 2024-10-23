import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions
import json
import requests
from datetime import datetime
from google.cloud import storage
import logging
import os 

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Dinesh Mote\Downloads\gcp-data-project-433112-aecffc0dc374.json"
logging.getLogger().setLevel(logging.INFO)

class FetchDataFromAPI(beam.DoFn):
    """
    Fetch data from API and output the parsed JSON.
    """
    def process(self, api_url):
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()
            yield data
        else:
            raise Exception(f"Failed to fetch data from API: {response.status_code}")


def convert_timestamp_to_gmt(timestamp_ms):
    """
    Convert Unix timestamp in milliseconds to GMT.
    """
    if timestamp_ms is not None:
        timestamp_s = timestamp_ms / 1000
        return datetime.utcfromtimestamp(timestamp_s).strftime('%Y-%m-%d %H:%M:%S')
    return None


class FlattenJSONData(beam.DoFn):
    """
    Flatten the JSON structure to prepare it for writing to BigQuery or other outputs.
    """
    def process(self, json_data):
        features = json_data.get("features", [])
        for feature in features:
            properties = feature["properties"]
            geometry = feature["geometry"]
            coordinates = geometry["coordinates"]

            flattened_record = {
                "place": properties.get("place"),
                "mag": float(properties.get("mag")) if properties.get("mag") is not None else None,
                "time": convert_timestamp_to_gmt(properties.get("time")),
                "updated": convert_timestamp_to_gmt(properties.get("updated")),
                "tz": properties.get("tz"),
                "url": properties.get("url"),
                "detail": properties.get("detail"),
                "felt": properties.get("felt"),
                "cdi": float(properties.get("cdi")) if properties.get("cdi") is not None else None,
                "mmi": float(properties.get("mmi")) if properties.get("mmi") is not None else None,
                "alert": properties.get("alert"),
                "status": properties.get("status"),
                "tsunami": properties.get("tsunami"),
                "sig": properties.get("sig"),
                "net": properties.get("net"),
                "code": properties.get("code"),
                "ids": properties.get("ids"),
                "sources": properties.get("sources"),
                "types": properties.get("types"),
                "nst": properties.get("nst"),
                "dmin": float(properties.get("dmin")) if properties.get("dmin") is not None else None,
                "rms": float(properties.get("rms")) if properties.get("rms") is not None else None,
                "gap": float(properties.get("gap")) if properties.get("gap") is not None else None,
                "magType": properties.get("magType"),
                "type": properties.get("type"),
                "title": properties.get("title"),
                "geometry": { 
                    "longitude": coordinates[0],
                    "latitude": coordinates[1],
                    "depth": float(coordinates[2]) if coordinates[2] is not None else None
                }
            }
            yield flattened_record


class AddColumnArea(beam.DoFn):
    """
    Add a column 'area' to the earthquake data based on the 'place'.
    """
    def process(self, record):
        place = record["place"]
        if "of" in place:
            area = place.split("of")[1].strip()  # Safely extract the area after 'of'
        else:
            area = "Unknown"  # Handle cases where 'of' is not found
        record["area"] = area
        yield record

def run():
    # Set up Beam pipeline options
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'gcp-data-project-433112'
    google_cloud_options.job_name = 'api-data-to-gcs'
    google_cloud_options.region = "us-east4"
    google_cloud_options.temp_location = 'gs://earthquake_analysis_data_bucket/stage_loc'
    google_cloud_options.staging_location = 'gs://earthquake_analysis_data_bucket/temp_loc'
    
    worker_options = options.view_as(WorkerOptions)
    worker_options.machine_type = 'n1-standard-4'
    
    # Define API URL and GCS bucket  
    api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
    bucket_name = "earthquake_analysis_data_bucket"
    current_date = datetime.now().strftime('%Y%m%d')
    
    bronze_output_path = f"gs://{bucket_name}/beam/landing/{current_date}/earthquake_raw"
    silver_output_path = f"gs://{bucket_name}/beam/silver/{current_date}/earthquake_transformed"
    
    with beam.Pipeline(options=options) as p:
        # Fetch data from API and write raw data to GCS
        raw_data = (p
                    | 'Create API URL' >> beam.Create([api_url])
                    | 'Fetch Data from API' >> beam.ParDo(FetchDataFromAPI())
                    #  | 'Log Raw Data' >> beam.Map(lambda x: logging.info(f"Raw data fetched: {x}"))
                   )
        
        # Write raw data to GCS
        write_raw_data_to_gcs = ( raw_data
                                 | "Format To JSON" >> beam.Map(lambda x: json.dumps(x))
                                 | "Write Raw Data to GCS" >> beam.io.WriteToText(bronze_output_path, 
                                    file_name_suffix=".json", 
                                    shard_name_template=""
                                    )
                                #   | "Log Raw Data Write" >> beam.Map(lambda x: logging.info(f"Raw data written to GCS: {bronze_output_path}"))
                               )
        
        # Read raw data from GCS
        raw_data_from_gcs = (write_raw_data_to_gcs
                             | 'Read Raw Data from GCS' >> beam.io.ReadFromText(bronze_output_path +"*.json")
                             | 'Parse JSON' >> beam.Map(json.loads)
                            #  | 'Log Read Data' >> beam.Map(lambda x: logging.info(f"Raw data read from GCS: {x}"))
                            )

        # Transform and flatten the data
        transformed_data = (raw_data_from_gcs
                            | 'Flatten JSON Data' >> beam.ParDo(FlattenJSONData())
                           )
        
        # Add 'area' column based on the 'place' value
        transformed_with_area = (transformed_data
                                 | 'Add Column Area' >> beam.ParDo(AddColumnArea())
                                )

        # Write transformed data to GCS
        (transformed_with_area
         | 'Write Transformed Data to GCS' >> beam.io.WriteToText(silver_output_path, 
                                                                   file_name_suffix=".json", 
                                                                   shard_name_template="", 
                                                                   num_shards=1)
        )

if __name__ == "__main__":
    run()
