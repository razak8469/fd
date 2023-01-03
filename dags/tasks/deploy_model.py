from airflow import DAG
from airflow.operators.bash import BashOperator

def deploy_model(dag: DAG, **kwargs) -> BashOperator:

    deploy_model_task = BashOperator(
        task_id="deploy_model_task",
        bash_command="scripts/deploy_model.sh",
        params = dict({
            "PROJECT_ID": "{{var.value.project_id}}",
            "MODEL_BUCKET": "{{var.value.model_bucket}}",
            "SIMPLE_MODEL": "{{va.value.simple_model}}",
            "AI_MODEL_NAME": "{{var.value.model_name}}",
            "VERSION_SIMPLE": "{{var.value.version_simple}}",
            }
        )
    )
    return deploy_model_task
