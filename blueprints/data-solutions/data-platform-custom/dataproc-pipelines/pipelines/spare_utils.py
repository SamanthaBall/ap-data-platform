# # Group 1: Data Cleaning
# # Clean text by removing URLs and special characters
# cleaned_tweets_df = tweets_df.withColumn(
#     'cleaned_text', 
#     regexp_replace(col('text'), r'http\S+|[^a-zA-Z\s]', '')
# )

# # Filter out retweets
# cleaned_tweets_df = cleaned_tweets_df.filter(~col('is_retweet'))

# # Group 2: Data Enrichment
# # Example: Add geolocation data (this could be done by calling a geocoding function)
# enriched_tweets_df = cleaned_tweets_df.withColumn(
#     'location', when(col('location').isNull(), 'Unknown').otherwise(col('location'))
# )

# # Example: Enrich user profile with a field for whether the user is verified
# enriched_tweets_df = enriched_tweets_df.withColumn(
#     'is_verified', when(col('user.verified') == True, 'Yes').otherwise('No')
# )

# # Group 3: Data Transformation
# # Example: Flatten nested fields for easier analysis
# final_tweets_df = enriched_tweets_df.select(
#     col('cleaned_text'),
#     col('user.name').alias('user_name'),
#     col('location'),
#     col('is_verified')
# )