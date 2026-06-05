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
    """Exécute un script Python du projet."""
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
    dag_id="pipeline_clubs",
    default_args=default_args,
    description="Pipeline complet clubs : collecte API → ETL → classements",
    schedule_interval="0 6 * * 1",  # Chaque lundi à 6h
    catchup=False,
    tags=["clubs", "etl", "football"],
) as dag:

    t1_collect = PythonOperator(
        task_id="collect_api_matches",
        python_callable=run_script,
        op_args=["src/ingestion/collect_api_matches.py"],
    )

    t2_transform = PythonOperator(
        task_id="transform_matches",
        python_callable=run_script,
        op_args=["src/transformation/transform_matches.py"],
    )

    t3_unified = PythonOperator(
        task_id="build_unified_matches",
        python_callable=run_script,
        op_args=["src/transformation/build_unified_matches.py"],
    )

    t4_classements = PythonOperator(
        task_id="build_classements",
        python_callable=run_script,
        op_args=["src/transformation/build_classements_unified.py"],
    )

    t5_clubs = PythonOperator(
        task_id="build_clubs_unified",
        python_callable=run_script,
        op_args=["src/transformation/build_clubs_unified.py"],
    )

    # Ordre d'exécution
    t1_collect >> t2_transform >> t3_unified >> t4_classements >> t5_clubs