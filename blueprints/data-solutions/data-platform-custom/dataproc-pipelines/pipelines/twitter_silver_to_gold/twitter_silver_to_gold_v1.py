import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

# Initialize Spark session with BigQuery connector
spark = SparkSession.builder.appName("TwitterSilverToGoldPipeline") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0") \
    .getOrCreate()

# Define paths and table names
silver_path = "output/" #"gs://your-silver-layer-bucket/path/to/silver-data/"
project_id = os.getenv("PROJECT_ID")
dataset_id = "gold"
table_name = "twitter2"

# Load Parquet data from GCS
silver_df = spark.read.parquet(silver_path)

# Apply any necessary transformations here
transformed_df = silver_df.select("id", "url", "text")  # Example selection

# Write the transformed data to BigQuery
transformed_df.write \
    .format("bigquery") \
    .option("table", f"{project_id}:{dataset_id}.{table_name}") \
    .option("writeMethod", "direct") \
    .mode("overwrite") \
    .save()

# Stop the Spark session
spark.stop()
