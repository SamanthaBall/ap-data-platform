import apache_beam as beam
from utils.cleaning.clean import drop_irrelevant_fields, clean_text, filter_retweets
from utils.enrichment.enrich import enrich_location, enrich_user_profile
from utils.flattening.flatten import flatten
from utils.transformation.transform import transform_tweet


with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions()) as p:
    
    # Step 1: Ingest data
    tweets = p | 'ReadFromSource' >> beam.io.ReadFromText('path_to_twitter_data.json')

    # Group 1: Data Cleaning
    cleaned_tweets = (
        tweets 
        | 'DropDuplicatesById' >> beam.Distinct(key=lambda x: x['id'])
        | 'FlattenJson' >> beam.ParDo(flatten())
        | 'DropIrrelevantFields' >> beam.Map(drop_irrelevant_fields)
        | 'Clean Text' >> beam.Map(clean_text)
        | 'Filter Retweets' >> beam.Filter(filter_retweets)
    )

    # Group 2: Data Enrichment
    enriched_tweets = (
        cleaned_tweets 
        | 'Enrich Location' >> beam.Map(enrich_location)
        | 'Enrich User Profile' >> beam.Map(enrich_user_profile)
    )

    # Group 3: Data Transformation
    transformed_tweets = (
        enriched_tweets
        | 'Transform Tweet' >> beam.Map(transform_tweet)
    )

    # Step 4: Output the data
    transformed_tweets | 'WriteToSink' >> beam.io.WriteToText('path_to_output_data')
