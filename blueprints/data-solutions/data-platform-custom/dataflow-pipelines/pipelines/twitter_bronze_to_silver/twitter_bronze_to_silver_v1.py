import apache_beam as beam
import json
from utils.cleaning.clean import DropIrrelevantFields, SelectFields, clean_text, filter_retweets
from utils.enrichment.enrich import enrich_user_profile
from utils.enrichment.geocode import FuzzyMatchLocation, GeocodeLocation
from utils.flattening.flatten import flatten
from utils.transformation.transform import transform_tweet
import pyarrow as pa




fields_to_remove = ['type', 'entities', 'extendedEntities', 'twitterUrl', 'author', 'media']

selected_fields = ['id', 'userName'] #userDescription


def limit_print_count(count):
    def _limit_print(element):
        nonlocal count
        if count < 5:  # Limit to 5 prints
            print(element)
            count += 1
        return element
    return _limit_print


def debug_data(element):
    print(element)
    return element


# Define the schema using pyarrow
schema = pa.schema([
    pa.field('id', pa.string(), nullable=True),
    pa.field('userName', pa.string(), nullable=True),
    #pa.field('userDescription', pa.string()),
])



with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions()) as p:
    
    # Step 1: Ingest data
    tweets = p | 'ReadFromSource' >> beam.io.ReadFromText('data/dataset_tweet-scraper_2024-03-04_15-52-13-507_delimited.json')
    tweets_json = tweets | 'Parse JSON' >> beam.Map(json.loads)
    
    count = 0

    # Group 1: Data Cleaning
    cleaned_tweets = (
        tweets_json 
        #| 'DropDuplicatesById' >> beam.Distinct(key=lambda x: x['id'])
        | 'FlattenJson' >> beam.ParDo(flatten())
        | 'DropIrrelevantFields' >>  beam.ParDo(DropIrrelevantFields(fields_to_remove))
        | 'Select Fields' >> beam.ParDo(SelectFields(selected_fields))
        #| 'Clean Text' >> beam.Map(clean_text)
        #| 'Filter Retweets' >> beam.Filter(filter_retweets)
    )

    # Group 2: Data Enrichment
    #enriched_tweets = (
        #cleaned_tweets 
        #| 'Fuzzy Match Locations' >> beam.ParDo(FuzzyMatchLocation())  # Step 1: Fuzzy match
        #| 'Print After Processing' >> beam.Map(limit_print_count(count))
    #     | 'Geocode Unmatched Locations' >> beam.ParDo(GeocodeLocation())  # Step 2: Geocode only unmatched
    #     | 'Enrich User Profile' >> beam.Map(enrich_user_profile)
    #)

    # # Group 3: Data Transformation
    # transformed_tweets = (
    #     enriched_tweets
    #     | 'Transform Tweet' >> beam.Map(transform_tweet)
    # )


    sampled_tweets = (
        cleaned_tweets
        | 'Sample Data' >> beam.combiners.Sample.FixedSizeGlobally(10)
    )

    #sampled_tweets |'Inspect Data' >> beam.Map(debug_data)

    # Step 4: Output the data in Parquet
    output_path = "output/twitter_output.parquet"
    # does this write to one or many files?
    #sampled_tweets | 'WriteToSink' >> beam.io.WriteToParquet(output_path, schema=schema) # change back to transformed_tweets
    sampled_tweets | 'Write To CSV' >> beam.io.WriteToText('output/output_file',
      file_name_suffix='.csv',
      num_shards=1)
