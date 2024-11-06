import os
from google.cloud import dataproc_v1
from logging.logs import log_info, log_error

def trigger_dataproc_job(request):

    log_info("Starting Dataflow pipeline trigger")

    project_id = os.environ.get('PROJECT_ID')  
    region = os.environ.get('REGION')        

    client = dataproc_v1.JobControllerClient()

    # Define your PySpark job
    job = {
        "placement": {
            "cluster_name": None  # Not needed for serverless jobs
        },
        "pyspark_job": {
            "main_python_file_uri": "gs://dp-pipelines-gcs-0/twitter_bronze_to_silver_v1.py",  # Path to your PySpark script
            #"args": ["arg1", "arg2"],  # Add input and output here
        },
        "dataproc_serverless_config": {
            "machine_type": "n1-standard-4",
            "autoscaling_policy": "<autoscaling_policy_name>"
        }
    }

    # Submit the job
    result = client.submit_job(project_id=project_id, region=region, job=job)
    log_info("Dataproc job submitted", job_id=result.job_id)
    return f"Job submitted: {result.reference.job_id}"

