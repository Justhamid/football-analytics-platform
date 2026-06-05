from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
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
    description="Pipeline ML : features temporelles → entraînement → prédictions",
    schedule_interval="0 8 * * 1",  # Chaque lundi à 8h (après players)
    catchup=False,
    tags=["ml", "market_value", "football"],
) as dag:

    # Attendre que le pipeline players soit terminé
    wait_players = ExternalTaskSensor(
        task_id="wait_for_players_pipeline",
        external_dag_id="pipeline_players",
        external_task_id="build_players_enriched",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    t1_features = PythonOperator(
        task_id="build_features_temporal",
        python_callable=run_script,
        op_args=["src/ml/build_features_temporal.py"],
    )

    t2_train = PythonOperator(
        task_id="train_model_temporal",
        python_callable=run_script,
        op_args=["src/ml/train_model_temporal.py"],
    )

    wait_players >> t1_features >> t2_train