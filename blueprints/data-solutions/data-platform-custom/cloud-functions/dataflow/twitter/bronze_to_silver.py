import os
from google.cloud import dataflow_v1beta3
from logging.logs import log_info, log_error

def trigger_dataflow_pipeline(request):
    try:

        log_info("Starting Dataflow pipeline trigger")

        project_id = os.environ.get('PROJECT_ID')  
        region = os.environ.get('REGION')          
        template_path = os.environ.get('TEMPLATE_PATH')  # Path to your Dataflow template in GCS

        client = dataflow_v1beta3.JobsV1Beta3Client()

        # Define parameters for the Dataflow job
        job = {
            "job_type": "DATAFLOW_TEMPLATE",
            "parameters": {
                "inputFile": "gs://<your-bucket>/input_data.csv",
                "output": "gs://<your-bucket>/output_data"
            },
            "template_gcs_path": template_path,  
            "environment": {
                "temp_location": "gs://<your-bucket>/temp/",
                "machine_type": "n1-standard-4",
            }
        }
        # Submit the job
        response = client.create_job(project_id=project_id, region=region, job=job)
        log_info("Dataflow job submitted", job_id=response.job_id)
        return f"Dataflow job submitted: {response.job_id}"

    except Exception as e:
        log_error("Failed to trigger Dataflow pipeline", error=str(e))
        raise
