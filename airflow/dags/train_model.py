from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from datetime import datetime

with DAG(
    dag_id="train_model",
    schedule="0 20 * * 5",
    start_date=datetime(2025, 1, 1),
    catchup=False
    ) as dag:
    train_model = BashOperator(
        task_id="train_model",
        bash_command="python /opt/airflow/ml/train_script.py"
    )