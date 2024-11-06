import apache_beam as beam
import json
import os
from utils.cleaning.clean import DropIrrelevantFields, SelectFields, clean_text, filter_retweets
from apache_beam.options.pipeline_options import PipelineOptions
from logging.logs import log_info, log_error
import pyarrow as pa


fields_to_remove = ['type', 'entities', 'extendedEntities', 'twitterUrl', 'author', 'media']
selected_fields = ['id', 'url'] #userName, userDescription


def debug_data(element):
    print(element)
    return element


def format_for_csv(record):
    return f"{record['id']},{record['url']}"


# Define the schema using pyarrow
schema = pa.schema([
    pa.field('id', pa.string(), nullable=True),
    pa.field('url', pa.string(), nullable=True),
    #pa.field('userDescription', pa.string()),
])


with beam.Pipeline(options=PipelineOptions()) as p:
    
    input_path_gcp = 'gs://ap-twitter/input/dataset_tweet-scraper_2024-03-04_15-52-13-507_delimited.json'
    input_path = 'data/dataset_tweet-scraper_2024-03-04_15-52-13-507_delimited.json'
    
    # Step 1: Ingest data
    tweets = p | 'ReadFromSource' >> beam.io.ReadFromText(input_path)
    tweets_json = tweets | 'Parse JSON' >> beam.Map(json.loads)


    # Group 1: Data Cleaning
    cleaned_tweets = (
        tweets_json 
        #| 'DropDuplicatesById' >> beam.Distinct(key=lambda x: x['id'])
        #| 'FlattenJson' >> beam.ParDo(flatten())
        #| 'DropIrrelevantFields' >>  beam.ParDo(DropIrrelevantFields(fields_to_remove))
        #| 'Clean Text' >> beam.Map(clean_text)
        #| 'Filter Retweets' >> beam.Filter(filter_retweets)
         | 'Select Fields' >> beam.ParDo(SelectFields(selected_fields))
    )

    # Output to CSV
    # formatted_tweets  = cleaned_tweets | 'Format for CSV' >> beam.Map(format_for_csv) # seems like they are all in one list
    # #formatted_tweets |'Inspect Data' >> beam.Map(debug_data)
    # output_path_gcp =  'gs://ap-twitter/output'
    # output_path = 'output/output'
    # formatted_tweets  | 'Write to CSV' >> beam.io.WriteToText(output_path, file_name_suffix='.csv', header='id,url')

    # Step 4: Output the data in Parquet
    output_path = "output/twitter_output.parquet"
    cleaned_tweets | 'WriteToSink' >> beam.io.WriteToParquet(output_path, schema=schema) # change back to transformed_tweets
    log_info("Data written to parquet.")
  





