import apache_beam as beam
from utils.cleaning.clean import DropIrrelevantFields, clean_text, filter_retweets
from utils.enrichment.enrich import enrich_user_profile
from utils.enrichment.geocode import FuzzyMatchLocation, GeocodeLocation
from utils.flattening.flatten import flatten
from utils.transformation.transform import transform_tweet


fields_to_remove = ['type', 'entities', 'extendedEntities', 'twitterUrl', 'author', 'media']

with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions()) as p:
    
    # Step 1: Ingest data
    tweets = p | 'ReadFromSource' >> beam.io.ReadFromText('path_to_twitter_data.json')

    # Group 1: Data Cleaning
    cleaned_tweets = (
        tweets 
        | 'DropDuplicatesById' >> beam.Distinct(key=lambda x: x['id'])
        | 'FlattenJson' >> beam.ParDo(flatten())
        | 'DropIrrelevantFields' >>  beam.ParDo(DropIrrelevantFields(fields_to_remove))
        | 'Clean Text' >> beam.Map(clean_text)
        | 'Filter Retweets' >> beam.Filter(filter_retweets)
    )

    # Group 2: Data Enrichment
    enriched_tweets = (
        cleaned_tweets 
        | 'Fuzzy Match Locations' >> beam.ParDo(FuzzyMatchLocation())  # Step 1: Fuzzy match
        | 'Geocode Unmatched Locations' >> beam.ParDo(GeocodeLocation())  # Step 2: Geocode only unmatched
        | 'Enrich User Profile' >> beam.Map(enrich_user_profile)
    )

    # Group 3: Data Transformation
    transformed_tweets = (
        enriched_tweets
        | 'Transform Tweet' >> beam.Map(transform_tweet)
    )

    # Step 4: Output the data
    transformed_tweets | 'WriteToSink' >> beam.io.WriteToText('path_to_output_data')
