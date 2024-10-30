import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
import pyarrow.parquet as pq

class TransformToBigQueryDict(beam.DoFn):
    def process(self, element):
        # Customize transformations here to prepare data for BigQuery
        # Example: return a dictionary with transformed data
        yield {
            'column1': element['column1'],
            'column2': element['column2'],
            # Map all columns needed
        }

def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(GoogleCloudOptions).project = 'your-project-id'
    pipeline_options.view_as(GoogleCloudOptions).region = 'your-region'
    pipeline_options.view_as(GoogleCloudOptions).staging_location = 'gs://your-bucket/staging'
    pipeline_options.view_as(GoogleCloudOptions).temp_location = 'gs://your-bucket/temp'
    pipeline_options.view_as(GoogleCloudOptions).job_name = 'silver-to-gold-pipeline'
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Specify BigQuery table schema for the gold layer
    table_spec = 'your-project-id:gold_dataset.gold_table'
    table_schema = {
        'fields': [
            {'name': 'column1', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'column2', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            # Define schema for each field
        ]
    }

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p 
            | "Read Parquet from Silver" >> beam.io.ReadFromParquet('gs://your-silver-bucket/path/to/silver_data')
            | "Transform for BigQuery" >> beam.ParDo(TransformToBigQueryDict())
            | "Write to BigQuery Gold" >> beam.io.WriteToBigQuery(
                table_spec,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
