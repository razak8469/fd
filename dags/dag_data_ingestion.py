from datetime import  datetime
from airflow import models
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

START_DATE = datetime(2021, 1, 1)
PROJECT_ID = 'hardy-unison-369003'
REGION = 'us-west4'
FDH_BUCKET = 'gs://hardy-unison-369003-fraud-detection-handbook'
TEMP_BUCKET = f'{FDH_BUCKET}/temp'
STAGING_BUCKET = f'{FDH_BUCKET}/staging'

PIPELINE_CODE = f'{FDH_BUCKET}/codes/fd_data_ingestion.py'

default_args = {
    'dataflow_default_options': {
        "tempLocation": TEMP_BUCKET,
        "stagingLocation": STAGING_BUCKET
    }
}

with models.DAG(
    dag_id="fd-data-ingestion",
    default_args=default_args,
    start_date=START_DATE,
    catchup=False,
    # tags=["example"]
) as dag_native_python:
    start_python_job = BeamRunPythonPipelineOperator(
        task_id = "move-pkl-files-to-bigquery",
        runner="DataflowRunner",
        py_file=PIPELINE_CODE,
        pipeline_options={
            "temp_location": TEMP_BUCKET,
            "staging_location": STAGING_BUCKET,
        },
        py_requirements=["apache-beam[gcp]==2.40.0"],
        py_interpreter='python3',
        py_system_site_packages=False,
        dataflow_config={
            "job_name": "pkl-data-ingestion",
            "location": REGION,
            "wait_until_finished": False,
        },

    )

