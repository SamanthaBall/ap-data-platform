#from utils.enrichment.enrich import enrich_user_profile
#from utils.enrichment.geocode import FuzzyMatchLocation, GeocodeLocation
#from utils.flattening.flatten import flatten
#from utils.transformation.transform import transform_tweet


# def limit_print_count(count):
#     def _limit_print(element):
#         nonlocal count
#         if count < 5:  # Limit to 5 prints
#             print(element)
#             count += 1
#         return element
#     return _limit_print



# options = PipelineOptions(
#     runner='DataflowRunner',
#     project=os.getenv("PROJECT_ID"),
#     #job_name='unique-job-name',
#     temp_location='gs://ap-twitter/staging/',
#     staging_location='gs://ap-twitter/staging/',
#     region='europe-west1'
#     #streaming = False
#     )


# options = PipelineOptions(
#     runner='DirectRunner',
#     temp_location='C:\\Users\\SamanthaBall\\OneDrive - SYNTHESIS SOFTWARE TECHNOLOGIES (PTY) LTD\\Documents\\ap-data-platform\\blueprints\\data-solutions\\data-platform-custom\\dataflow-pipelines\\temp',
#     staging_location='C:\\Users\\SamanthaBall\\OneDrive - SYNTHESIS SOFTWARE TECHNOLOGIES (PTY) LTD\\Documents\\ap-data-platform\\blueprints\\data-solutions\\data-platform-custom\\dataflow-pipelines\\temp',
#     )


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


    # sampled_tweets = (
    #     cleaned_tweets
    #     | 'Sample Data' >> beam.combiners.Sample.FixedSizeGlobally(10)
    # )

    #sampled_tweets |'Inspect Data' >> beam.Map(debug_data)

    # Step 4: Output the data in Parquet
    #output_path = "output/twitter_output.parquet"
    # does this write to one or many files?
    #sampled_tweets | 'WriteToSink' >> beam.io.WriteToParquet(output_path, schema=schema) # change back to transformed_tweets
    #sampled_tweets | 'WriteToText' >> beam.io.WriteToText('output/output_file', file_name_suffix='.txt', num_shards=1)