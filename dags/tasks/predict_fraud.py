
from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

def predict_fraud(dag: DAG, **kwargs) -> BeamRunPythonPipelineOperator: 
    STREAMING_PIPELINE_CODE = kwargs.get('STREAMING_PIPELINE_CODE')
    TEMP_LOCATION = kwargs.get('TEMP_LOCATION')
    STAGING_LOCATION = kwargs.get('STAGING_LOCATION')
    REGION = kwargs.get('REGION')

    predict_fraud_task = BeamRunPythonPipelineOperator(
        task_id = "predict_fraud_task",
        runner="DataflowRunner",
        py_file=STREAMING_PIPELINE_CODE,
        pipeline_options={
            "temp_location": TEMP_LOCATION,
            "staging_location": STAGING_LOCATION,
        },
        py_requirements=["apache-beam[gcp]==2.40.0"],
        py_interpreter='python3',
        py_system_site_packages=False,
        dataflow_config={
            "job_name": "streaming-ml-inference",
            "location": REGION,
            "wait_until_finished": False,
        },
    )
    return predict_fraud_task
