import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions
import json
import requests
from datetime import datetime
from google.cloud import storage
import logging
from apache_beam.io.parquetio import WriteToParquet, ReadFromParquet
import os 
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import pyarrow as pa


# Set environment variable for Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Dinesh Mote\Downloads\gcp-data-project-440907-eb61e9727efa.json"
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


def log_data(record):
    logging.info("-----------------------------------------------")
    logging.info(record)
    logging.info("-----------------------------------------------")

    return record


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
                "place": str(properties.get("place")),
                "mag": float(properties.get("mag")) if properties.get("mag") is not None else None,
                "time": convert_timestamp_to_gmt(properties.get("time")),
                "updated": convert_timestamp_to_gmt(properties.get("updated")),
                "tz": int(properties.get("tz")) if properties.get("tz") is not None else None,  # Ensure this is an integer
                "url": properties.get("url"),
                "detail": properties.get("detail"),
                "felt": int(properties.get("felt")) if properties.get("felt") is not None else None,
                "cdi": float(properties.get("cdi")) if properties.get("cdi") is not None else None,
                "mmi": float(properties.get("mmi")) if properties.get("mmi") is not None else None,
                "alert": properties.get("alert"),
                "status": properties.get("status"),
                "tsunami": int(properties.get("tsunami")) if properties.get("tsunami") is not None else None,
                "sig": int(properties.get("sig")) if properties.get("sig") is not None else None,
                "net": properties.get("net"),
                "code": properties.get("code"),
                "ids": properties.get("ids"),
                "sources": properties.get("sources"),
                "types": properties.get("types"),
                "nst": int(properties.get("nst")) if properties.get("nst") is not None else None,
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
        
        
class AddInsertDate(beam.DoFn):
    """
    Add an insert_date column to each record with the current date in 'YYYY-MM-DD' format.
    """
    def process(self, record):
        record["insert_date"] = datetime.utcnow().strftime('%Y-%m-%d')
        yield record


def get_table_schema():
    # Define the table schema before it's used
    return {
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


def get_arrow_schema():
    # Define the Apache Arrow schema for Parquet
    return pa.schema([
        ("place", pa.string()),
        ("mag", pa.float32()),
        ("time", pa.string()),
        ("updated", pa.string()),
        ("tz", pa.int32()),
        ("url", pa.string()),
        ("detail", pa.string()),
        ("felt", pa.int32()),
        ("cdi", pa.float32()),
        ("mmi", pa.float32()),
        ("alert", pa.string()),
        ("status", pa.string()),
        ("tsunami", pa.int32()),
        ("sig", pa.int32()),
        ("net", pa.string()),
        ("code", pa.string()),
        ("ids", pa.string()),
        ("sources", pa.string()),
        ("types", pa.string()),
        ("nst", pa.int32()),
        ("dmin", pa.float32()),
        ("rms", pa.float32()),
        ("gap", pa.float32()),
        ("magType", pa.string()),
        ("type", pa.string()),
        ("title", pa.string()),
        ("geometry", pa.struct([
            ("longitude", pa.float32()),
            ("latitude", pa.float32()),
            ("depth", pa.float32())
        ])),
        ("area", pa.string())
        ])