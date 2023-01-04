from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from scripts.publish_to_pubsub import publish_to_pubsub

def publish_transactions(dag: DAG, **kwargs) -> TaskGroup:
    PROJECT_ID = kwargs.get("PROJECT_ID", None)
    DATASET_OUT = kwargs.get("DATASET_OUT", None)
    TABLE_OUT = kwargs.get('TABLE_OUT', None)
    REGION = kwargs.get("REGION", None)
    BATCH_PIPELINE_CODE = kwargs.get('BATCH_PIPELINE_CODE', None)
    TEMP_LOCATION = kwargs.get('TEMP_LOCATION', None)
    STAGING_LOCATION = kwargs.get('STAGING_LOCATION', None)
    TRANSACTIONS_DATASET = kwargs.get("TRANSACTIONS_DATASET", None)
    NEW_TRANSACTIONS_TABLE = kwargs.get("NEW_TRANSACTIONS_TABLE", None)
    STREAMING_TRANSACTIONS_TOPIC = kwargs.get("STREAMING_TRANSACTIONS_TOPIC", None)

    publish_transactions_group = TaskGroup(group_id="publish_transactions_group")

    ingest_transactions_task = BeamRunPythonPipelineOperator(
        task_id = "ingest_new_transactions_task",
        task_group = publish_transactions_group,
        runner="DataflowRunner",
        py_file=BATCH_PIPELINE_CODE,
        pipeline_options={
            "temp_location": TEMP_LOCATION,
            "staging_location": STAGING_LOCATION,
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
    
    extract_transactions_features_task = BigQueryInsertJobOperator(
        task_id="extract_new_transactions_features_task",
        task_group=publish_transactions_group,
        configuration={
            "query": {
                "query": "{% include 'scripts/extract_features.sql' %}",
                'destinationTable': {
                    'projectId': PROJECT_ID,
                    'datasetId': DATASET_OUT,
                    'tableId': TABLE_OUT
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "useLegacySql": False,
            }
        },
        location=REGION,
    )


    publish_to_pubsub_task = PythonOperator(
        task_id="publish_to_pubsub_task",
        python_callable=publish_to_pubsub,
        op_kwargs = {
            'PROJECT_ID': PROJECT_ID,
            'DATASET': TRANSACTIONS_DATASET,
            'TABLE': NEW_TRANSACTIONS_TABLE,
            'TOPIC': STREAMING_TRANSACTIONS_TOPIC,
        }
    )

    ingest_transactions_task >> extract_transactions_features_task >> publish_to_pubsub_task

    return publish_transactions_group
