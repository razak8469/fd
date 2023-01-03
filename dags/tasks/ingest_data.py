from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

def ingest_data(dag: DAG, **kwargs) -> BeamRunPythonPipelineOperator:
    py_file = kwargs.get('PIPELINE_CODE')
    temp_location = kwargs.get('TEMP_LOCATION')
    staging_location = kwargs.get('STAGING_LOCATION')
    region = kwargs.get('REGION')
    
    ingest_data_task = BeamRunPythonPipelineOperator(
        task_id = "ingest_data_task",
        runner="DataflowRunner",
        py_file=py_file,
        pipeline_options={
            "temp_location": temp_location,
            "staging_location": staging_location,
        },
        py_requirements=["apache-beam[gcp]==2.40.0"],
        py_interpreter='python3',
        py_system_site_packages=False,
        dataflow_config={
            "job_name": "pkl-data-ingestion",
            "location": region,
            "wait_until_finished": False,
        },
    )
    return ingest_data_task
