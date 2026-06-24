import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys

default_args = {
    "owner":            "football_analytics",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

def run_script(script_path: str) -> None:
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True,
        cwd="/opt/airflow"
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Script échoué : {result.stderr}")

with DAG(
    dag_id="pipeline_ml",
    default_args=default_args,
    description="Pipeline ML : projection de carrière des jeunes joueurs (16-21 ans)",
    schedule_interval="0 8 * * 1",  # Chaque lundi à 8h (après pipeline_players)
    catchup=False,
    tags=["ml", "projection", "carriere", "football"],
) as dag:

    t1_projection = PythonOperator(
        task_id="train_model_projection",
        python_callable=run_script,
        op_args=["src/ml/train_model_projection.py"],
    )