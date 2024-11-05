import os
import time
from google.cloud import dataflow_v1beta3

def trigger_dataflow_pipeline(request):
    # Read environment variables
    project_id = os.environ.get('PROJECT_ID')  # Access the PROJECT_ID variable
    region = os.environ.get('REGION')          # Access the REGION variable
    template_path = os.environ.get('TEMPLATE_PATH')  # Path to your Dataflow template in GCS

    client = dataflow_v1beta3.JobsV1Beta3Client()

    # Define parameters for the Dataflow job
    job = {
        "job_type": "DATAFLOW_TEMPLATE",
        "parameters": {
            "inputFile": "gs://<your-bucket>/input_data.csv",
            "output": "gs://<your-bucket>/output_data"
        },
        "template_gcs_path": template_path,  # Path to your Dataflow template
        "environment": {
            "temp_location": "gs://<your-bucket>/temp/",
            "machine_type": "n1-standard-4",
        }
    }
    # Submit the job
    response = client.create_job(project_id=project_id, region=region, job=job)
    
    return f"Dataflow job submitted: {response.job_id}"
