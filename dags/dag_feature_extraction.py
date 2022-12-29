from datetime import datetime, timedelta
 
from airflow import models
from airflow.providers.google.cloud.operators import bigquery
from airflow.operators.bash import BashOperator

PROJECT_ID = 'hardy-unison-369003'
REGION = 'US'


DATASET_IN = 'fd_raw'
TABLE_IN = 'transactions'

DATASET_OUT = 'fd_model'
TABLE_OUT = 'transactions_features'

yesterday = datetime.combine(
    datetime.today() - timedelta(1),datetime.min.time()
)

with models.DAG(
    dag_id="_extract-features-dag",
    start_date=yesterday,
    catchup=False,
    user_defined_macros={"DATASET_IN": DATASET_IN, "TABLE_IN": TABLE_IN},
) as dag:

    extract_features = bigquery.BigQueryInsertJobOperator(
        task_id="extract_features",
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

