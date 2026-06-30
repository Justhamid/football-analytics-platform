import os
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def on_failure_callback(context):
    """Callback exécuté automatiquement en cas d'échec d'une tâche."""
    from airflow.utils.email import send_email

    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    log_url  = context["task_instance"].log_url
    exc      = context.get("exception", "Erreur inconnue")

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
        html_content=body
    )


default_args = {
    "owner":               "football_analytics",
    "depends_on_past":     False,
    "start_date":          datetime(2024, 1, 1),
    "email_on_failure":    True,
    "email_on_retry":      False,
    "email":               [os.getenv("AIRFLOW_ALERT_EMAIL",
                             "hamidbelhadjkacem@gmail.com")],
    "retries":             1,
    "retry_delay":         timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}


def run_script(script_path: str) -> None:
    """Exécute un script Python du projet."""
    env = os.environ.copy()
    env["PYTHONPATH"] = "/opt/airflow"

    result = subprocess.run(
        ["python", script_path],
        capture_output=True,
        text=True,
        cwd="/opt/airflow",
        env=env
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Script échoué : {result.stderr}")


with DAG(
    dag_id="pipeline_clubs",
    default_args=default_args,
    description="Pipeline complet clubs : collecte API → ETL → classements",
    schedule_interval="0 6 * * 1",
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

    t1_collect >> t2_transform >> t3_unified >> t4_classements >> t5_clubs
