from datetime import datetime, timedelta
import os
import subprocess
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


def on_failure_callback(context):
    """Callback exécuté automatiquement en cas d'échec d'une tâche."""
    from airflow.utils.email import send_email

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    log_url = context["task_instance"].log_url
    exc = context.get("exception", "Erreur inconnue")

    subject = f"[Football Analytics] ❌ Échec : {dag_id}.{task_id}"
    body = f"""
    <h3>⚠️ Échec de tâche Airflow</h3>
    <table>
        <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
        <tr><td><b>Tâche</b></td><td>{task_id}</td></tr>
        <tr><td><b>Date</b></td><td>{context['execution_date']}</td></tr>
        <tr><td><b>Erreur</b></td><td>{exc}</td></tr>
        <tr><td><b>Logs</b></td><td><a href="{log_url}">{log_url}</a></td></tr>
    </table>
    <p>Action requise : vérifier les logs et relancer la tâche si nécessaire.</p>
    """
    send_email(
        to=os.getenv("AIRFLOW_ALERT_EMAIL", "hamidbelhadjkacem@gmail.com"),
        subject=subject,
        html_content=body,
    )


default_args = {
    "owner": "football_analytics",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": [
        os.getenv("AIRFLOW_ALERT_EMAIL", "hamidbelhadjkacem@gmail.com")
    ],
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}


def run_script(script_path: str) -> None:
    """Exécute un script Python du projet en diffusant la sortie stdout en temps réel."""
    env = os.environ.copy()
    env["PYTHONPATH"] = "/opt/airflow"

    # Popen + '-u' permet d'envoyer les logs au fur et a mesure et d'eviter la coupure reseau
    process = subprocess.Popen(
        ["python", "-u", script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd="/opt/airflow",
        env=env,
    )

    # Affichage en direct dans le terminal/logs Airflow
    if process.stdout:
        for line in process.stdout:
            print(line, end="")
            sys.stdout.flush()

    process.wait()

    if process.returncode != 0:
        raise Exception(
            f"Script échoué ({script_path}) avec le code d'erreur : {process.returncode}"
        )


with DAG(
    dag_id="pipeline_players",
    default_args=default_args,
    description="Pipeline joueurs : refresh Kaggle → ETL Transfermarkt → performances → enrichissement",
    schedule_interval="0 7 * * 1",
    catchup=False,
    tags=["players", "etl", "football"],
) as dag:

    t0_refresh = PythonOperator(
        task_id="refresh_transfermarkt_source",
        python_callable=run_script,
        op_args=["src/ingestion/refresh_transfermarkt_source.py"],
    )

    t1_transform = PythonOperator(
        task_id="transform_players",
        python_callable=run_script,
        op_args=["src/transformation/transform_players.py"],
    )

    t1b_wait_clubs = ExternalTaskSensor(
        task_id="wait_for_pipeline_clubs",
        external_dag_id="pipeline_clubs",
        external_task_id="build_clubs_unified",
        execution_delta=timedelta(hours=1),
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
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

    (
        t0_refresh
        >> t1_transform
        >> t1b_wait_clubs
        >> t2_appearances
        >> t3_enriched
    )