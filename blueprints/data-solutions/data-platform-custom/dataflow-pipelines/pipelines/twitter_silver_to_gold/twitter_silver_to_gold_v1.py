import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions
from dotenv import load_dotenv

load_dotenv()

class TransformToBigQueryDict(beam.DoFn):
    def process(self, element):
        # Customize transformations here to prepare data for BigQuery
        # Example: return a dictionary with transformed data
        yield {
            'id': element['id'],
            'url': element['url'],
            # Map all columns needed
        }


options = PipelineOptions(
    project = os.getenv("PROJECT_ID"),
    region = 'europe-west1',
    runner='DirectRunner',
    temp_location='gs://ap-twitter/temp/',
)

# Specify BigQuery table schema for the gold layer
table_spec = os.getenv("PROJECT_ID") + ':gold.twitter'
table_schema = {
    'fields': [
        {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'url', 'type': 'STRING', 'mode': 'NULLABLE'},
        # Define schema for each field
    ]
}

with beam.Pipeline(options=options) as p:
    (
        p 
        | "Read Parquet from Silver" >> beam.io.ReadFromParquet('output/twitter_output.parquet-00000-of-00001')
        | "Transform for BigQuery" >> beam.ParDo(TransformToBigQueryDict())
        | "Write to BigQuery Gold" >> beam.io.WriteToBigQuery(
            table_spec,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
    )

