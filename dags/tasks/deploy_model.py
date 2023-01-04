from airflow import DAG
from airflow.operators.bash import BashOperator

def deploy_model(dag: DAG, **kwargs) -> BashOperator:

    PROJECT_ID = kwargs.get('PROJECT_ID', None)
    REGION = kwargs.get('REGION', None)
    MODLE_DATASET_ID = kwargs.get('MODLE_DATASET_ID', None)
    MODEL_NAME = kwargs.get('MODEL_NAME', None)
    MODEL_BUCKET = kwargs.get('MODEL_BUCKET', None)
    MODEL_NAME = kwargs.get('MODEL_NAME', None)
    MODEL_VERSION = kwargs.get('MODEL_VERSION', None)
    ENDPOINT_ID = kwargs.get('ENDPOINT_ID', None)
    MODEL_ID = kwargs.get('MODEL_ID', None)
    ENDPOINT_NAME = kwargs.get('ENDPOINT_NAME', None)
    IMAGE_URI = kwargs.get('IMAGE_URI', None)

    deploy_model_task = BashOperator(
        task_id="deploy_model_task",
        bash_command="scripts/deploy_model.sh",
        params = dict({
            "PROJECT_ID": PROJECT_ID,
            "REGION": REGION,
            "MODLE_DATASET_ID": MODLE_DATASET_ID,
            "MODEL_NAME": MODEL_NAME,
            "MODEL_BUCKET": MODEL_BUCKET,
            "MODEL_NAME": MODEL_NAME,
            "MODEL_VERSION": MODEL_VERSION,
            "ENDPOINT_ID": ENDPOINT_ID,
            "MODEL_ID": MODEL_ID,
            "ENDPOINT_NAME": ENDPOINT_NAME,
            "IMAGE_URI": IMAGE_URI,
            }
        )
    )
    return deploy_model_task
