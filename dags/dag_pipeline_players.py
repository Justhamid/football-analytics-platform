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
    dag_id="pipeline_players",
    default_args=default_args,
    description="Pipeline joueurs : ETL Transfermarkt → performances → enrichissement",
    schedule_interval="0 7 * * 1",  # Chaque lundi à 7h (après clubs)
    catchup=False,
    tags=["players", "etl", "football"],
) as dag:

    t1_transform = PythonOperator(
        task_id="transform_players",
        python_callable=run_script,
        op_args=["src/transformation/transform_players.py"],
    )

    t2_appearances = PythonOperator(
        task_id="build_appearances_unified",
        python_callable=run_script,
        op_args=["src/transformation/build_appearances_unified.py"],
    )

    t3_enriched = PythonOperator(
        task_id="build_players_enriched",
        python_callable=run_script,
        op_args=["src/transformation/build_players_enriched.py"],
    )

    t1_transform >> t2_appearances >> t3_enriched