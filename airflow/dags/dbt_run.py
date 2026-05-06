from datetime import datetime
from airflow.sdk import DAG
from cosmos.airflow.dag import DbtDag
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import os

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/data_transform"
DBT_EXEC_PATH = "/home/airflow/.local/bin/dbt"  # опционально

profile_config = ProfileConfig(
    profile_name="smartparking",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="pg_default",
        profile_args={"schema": "staging"}
    )
)

dbt_run_stg = DbtDag(
    dag_id="smart_parking",
    schedule="0 19 * * 5",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=DBT_EXEC_PATH),
    # все задачи dbt будут в одной группе, либо можно развернуть в DAG
    operator_args={
        "install_deps": True,      # установит зависимости dbt
    }
)