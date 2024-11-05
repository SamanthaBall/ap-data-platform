import functions_framework
from google.cloud import dataproc_v1

@functions_framework.http
def trigger_dataproc_job(request):
    client = dataproc_v1.JobControllerClient()
    project_id = "<your_project_id>"
    region = "<your_region>"
    job = {
        "placement": {
            "cluster_name": None  # Not needed for serverless jobs
        },
        "spark_job": {
            "main_class": "org.apache.spark.examples.SparkPi",
            "jar_file_uris": ["gs://<your_bucket>/jars/your-spark-job.jar"],
            "args": ["1000"],
        },
        "dataproc_serverless_config": {
            "machine_type": "n1-standard-4",
            "autoscaling_policy": "<autoscaling_policy_name>"
        }
    }

    result = client.submit_job(project_id=project_id, region=region, job=job)
    return f"Job submitted: {result.reference.job_id}"
