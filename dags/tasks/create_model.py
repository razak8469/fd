from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup

def create_model(dag: DAG, **kwargs) -> TaskGroup:
    PROJECT_ID = kwargs.get("PROJECT_ID")
    DATASET_OUT = kwargs.get("DATASET_OUT")
    TABLE_OUT = kwargs.get('TABLE_OUT')
    REGION = kwargs.get("REGION")

    # create_model_group = TaskGroup(group_id="create_model_group", args=kwargs)
    create_model_group = TaskGroup(group_id="create_model_group")

    extract_features_task = BigQueryInsertJobOperator(
        task_id="extract_features_task",
        task_group=create_model_group,
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

    create_model_task = BigQueryInsertJobOperator(
        task_id="create_model_task",
        task_group=create_model_group,
        configuration={
            "query": {
                "query": "{% include 'scripts/create_model.sql' %}",
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    extract_features_task >> create_model_task

    return create_model_group
