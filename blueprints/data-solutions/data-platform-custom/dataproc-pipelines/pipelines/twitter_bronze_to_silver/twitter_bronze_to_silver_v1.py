from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("TwitterBronzeToSilverPipeline").getOrCreate()

#spark.sparkContext.setLogLevel("DEBUG")

# Read the raw Twitter JSON data
tweets_df = spark.read.json("data/dataset_tweet-scraper_2024-03-04_15-52-13-507_delimited.json")

# Select specific columns
selected_columns_df = tweets_df.select("id", "url", "text", "lang")  # Replace with actual column names

# Show selected data (for testing, remove in production)
selected_columns_df.show()

# Write back to GCS in parquet format, or process further as needed
output_path = "output"
selected_columns_df.write.mode('overwrite').parquet(output_path)
#selected_columns_df.write.mode('overwrite').csv(output_path)

# Stop the Spark session
spark.stop()



