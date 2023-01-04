from airflow import DAG
from airflow.utils.dates import days_ago
from tasks.ingest_data import ingest_data
from tasks.create_model import  create_model
from tasks.deploy_model import deploy_model
from tasks.publish_transactions import publish_transactions
from tasks.predict_fraud import predict_fraud


with DAG(
    dag_id="_credit_card_fraud_detection_demo",
    # schedule_interval="@once",
    schedule_interval=None,
    start_date=days_ago(1),
) as dag:

    PROJECT_ID = "{{var.value.project_id}}"
    REGION = "{{var.value.region}}"
    SOURCE_BUCKET = "{{var.value.source_bucket}}"
    TEMP_BUCKET = f'{SOURCE_BUCKET}/temp'
    STAGING_BUCKET = f'{SOURCE_BUCKET}/staging'
    DATASET_OUT = "{{var.value.dataset_out}}"
    TABLE_OUT = "{{var.value.table_out}}"
    BATCH_PIPELINE_CODE = "{{var.value.batch_pipeline_code}}"
    STREAMING_PIPELINE_CODE = "{{var.value.streaming_pipeline_code}}"

    kwargs = {
        "PROJECT_ID": PROJECT_ID,
        "REGION": REGION,
        "SOURCE_BUCKET": SOURCE_BUCKET,
        "TEMP_BUCKET": TEMP_BUCKET,
        "STAGING_BUCKET": STAGING_BUCKET,
        "DATASET_OUT": DATASET_OUT,
        "TABLE_OUT": TABLE_OUT,
        "BATCH_PIPELINE_CODE": BATCH_PIPELINE_CODE,
        "STREAMING_PIPELINE_CODE": STREAMING_PIPELINE_CODE,
    }

    # ingest_data_task = ingest_data(dag=dag, kwargs=kwargs)

    # create_model_taskgroup = create_model(dag=dag, kwargs=kwargs)

    deploy_model_task = deploy_model(dag=dag, kwargs=kwargs)

    publish_transactions_taskgroup = publish_transactions(dag=dag, kwargs=kwargs)

    predict_fraud_task = predict_fraud(dag=dag, kwargs=kwargs)

    # ingest_data_task >> create_model_taskgroup >> deploy_model_task

    [deploy_model_task, publish_transactions_taskgroup] >> predict_fraud_task
    
