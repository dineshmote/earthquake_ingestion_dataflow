import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions
from datetime import datetime
from google.cloud import storage
import logging
import os 
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from datetime import datetime, timedelta

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Dinesh Mote\Downloads\gcp-data-project-440907-eb61e9727efa.json"
logging.getLogger().setLevel(logging.INFO)


def log_data(record):
    logging.info("-----------------------------------------------")
    logging.info(record)
    logging.info("-----------------------------------------------")

    return record

class ExtractRegionData(beam.DoFn):
    def process(self, element):
        region = element.get('area')
        if region:
            yield (region,1)
        
        

class Avg_Mag_By_Region(beam.DoFn):
    def process(self, element):
        region = element["area"]
        mag = element["mag"]
        yield (region, (mag, 1))
        
        
class ExtractDate(beam.DoFn):
    def process(self, element):
        date = element["time"].date().strftime("%Y-%m-%d")
        yield (date, 1)
                
class ExtractRegionAndDate(beam.DoFn):
    def process(self, element):
        region = element["area"]
        date = element["time"].date().strftime("%Y-%m-%d")
        yield ((date,region),1)

def calculate_average(counts):
    total_earthquakes = sum (counts)
    num_days = len(counts)
    return total_earthquakes / num_days if num_days > 0 else 0

def filter_last_week(element):
    current_date = datetime.now().date()
    last_week_date = current_date - timedelta(days=7)
    earthquake_date = element['time'].date()
    return last_week_date <= earthquake_date <= current_date


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
    
    table_spec = 'gcp-data-project-440907:earthquake_ingestion.earthquake_table_dataflow'
    
   
    with beam.Pipeline(options=options) as p:
        # Read data from BigQuery
        bq_data = (p
                    | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(table=table_spec)
                    # | 'ConvertToDict' >> beam.Map(lambda row: dict(row))
                    # | 'Log Data' >> beam.Map(log_data)
                   )
        # 1. Count the number of earthquakes by region
        # num_of_earthquake_by_region = (
            # bq_data
            # | 'Count Earthquakes by Region' >> beam.ParDo(ExtractRegionData())
            # | 'Group by Region' >> beam.CombinePerKey(sum)
            # |'Log Data' >> beam.Map(log_data)

        # )
        
        # 2. Find the average magnitude by the region
        # avg_mag_by_region =( 
        #     bq_data |"Extract Mag and Region" >> beam.ParDo(Avg_Mag_By_Region())
        #     |"Sum Mag By Region" >> beam.CombinePerKey(lambda mags:(sum(m[0] for m in mags), sum(m[1] for m in mags)))
        #     |"Avg Mag By Region" >>beam.Map(lambda x:(x[0], x[1][0]/x[1][1] if x[1][1] > 0 else 0))  
        #     |"log data" >> beam.Map(log_data)
        #     ) 
        
        # #3. Find how many earthquakes happen on the same day
        # earthquakes_same_day = (
        #     bq_data
        #     | "Extract Date" >> beam.ParDo(ExtractDate())
        #     | "Group by Date" >> beam.CombinePerKey(sum)
        #     | "Log Data1" >> beam.Map(log_data)
        # )
        
        # #4. Find how many earthquakes happen on same day and in same region
        # earthquakes_by_day_region = (
        #     bq_data
        #     | "Extract Date and Region" >> beam.ParDo(ExtractRegionAndDate())
        #     | "Group by Date and Region" >> beam.CombinePerKey(sum)
        #     | "Log Data2" >> beam.Map(log_data)
        # )
        
        # #5. Find average earthquakes happen on the same day
        # average_earthquakes_per_day = (
        #     bq_data
        #     | "Extract Date" >> beam.ParDo(ExtractDate())
        #     | "Group by Date" >> beam.CombinePerKey(sum)
        #     | 'Calculate Average Earthquakes per Day' >> beam.combiners.Mean.PerKey()
        #     | "Log Data3" >> beam.Map(print)
        # )
        
        # #6. Find average earthquakes happen on same day and in same region
        # average_earthquakes_per_day_by_area =(
        #     bq_data
        #     | 'Extract Date, Area and Count' >> beam.ParDo(ExtractRegionAndDate())
        #     | 'Count Earthquakes per Date and Area' >> beam.CombinePerKey(sum)
        #     | 'Calculate Average Earthquakes per Date and Area' >> beam.combiners.Mean.PerKey()
        #     | "Log Data6" >>beam.Map(print)
        #     | "Count Final Records" >> beam.combiners.Count.Globally()
        #     | "Count Log" >>beam.Map(print)
        #      )
        
        #7. Find the region name, which had the highest magnitude earthquake last week
        
        # highest_magnitude_earthquake_last_week =(
        #     bq_data 
        #     | "Filter Last Week Records" >> beam.Filter(filter_last_week)
        #     | "Get Max Magnitude Earthquake" >> beam.combiners.Top.Of(1, key= lambda record:record['mag'])
        #     | "Extract Region" >> beam.Map(lambda records:records[0]['area'] if records else None)
        #     | "Log data7" >> beam.Map(print)
        # )
        
        # #8. Find the region name, which is having magnitudes higher than 5
        
        # region_with_high_mag =(
        #     bq_data
        #     | "Filter Magnitude Higher than 5" >> beam.Filter(lambda record:record['mag']>5)
        #     | "Region Name" >> beam.Map(lambda record:record['area'])
        #     | "Distinct Region" >> beam.Distinct()
        #     | "Log data8" >> beam.Map(print)
        #     | "Count Records" >> beam.combiners.Count.Globally()
        #     | "Count Log" >> beam.Map(print)
        # )
        
        # 9. Find out the regions which are having the highest frequency and intensity of earthquakes
        regions_with_highest_freq_and_intensity =( 
            bq_data
            | "Extract Region and Magnitude" >> beam.Map(lambda record: (record['area'], record['mag']))
            | "Group by Region" >> beam.GroupByKey()
            | "Highest Frequency and Intesity" >> beam.Map(lambda x:(x[0], len(x[1]), max(x[1])))
            # | "Calculate Average Magnitude" >> beam.combiners.Mean.PerKey()
            | "Log data9" >> beam.Map(print)             
            | "HFind Top Region by Frequency" >> beam.combiners.Top.Of(1,key= lambda x:x[1])
            | "Highest Count" >> beam.Map(print)
        )


    
        
       

if __name__ == "__main__":
    run()
