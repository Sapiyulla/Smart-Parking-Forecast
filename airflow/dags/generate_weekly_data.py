from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, task
from datetime import datetime
import subprocess
import sys

def run_weekly_generator():
    generator_dir = "/opt/airflow/generators/weekly"
    result = subprocess.run(
        [sys.executable, "weekly_generator.py"],
        capture_output=True,
        text=True,
        cwd=generator_dir  # ← здесь лежит weekly_generation.config.yml
    )
    if result.returncode != 0:
        raise Exception(f"Generator failed: {result.stderr}")
    print(result.stdout)
    
with DAG(
    dag_id="generator_weekly_data",
    schedule="0 18 * * 5",
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    run_generator = PythonOperator(
        task_id="run_generator",
        python_callable=run_weekly_generator
    )
    
    run_generator #type: ignore