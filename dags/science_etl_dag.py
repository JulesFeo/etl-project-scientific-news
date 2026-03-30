import sys
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

CONFIG_PATH = "/opt/airflow/config/config.yaml"


def _load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_etl(**context):
    from src.pipeline import run

    config = _load_config()
    results = run(config)
    context["ti"].xcom_push(key="etl_results", value=results)
    return results


def send_notification(**context):
    from src.logger import setup_logger
    from src.notify import send_telegram

    config = _load_config()
    results = context["ti"].xcom_pull(task_ids="run_etl_pipeline", key="etl_results")

    etl_id = results.get("etl_id", "notification")
    logger = setup_logger(config, etl_id)

    send_telegram(config, results, logger)


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="science_tech_etl",
    default_args=default_args,
    description="Multi-source science & tech ETL pipeline with Telegram notifications",
    schedule="@daily",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["etl", "science", "openalex", "arxiv", "pubmed"],
) as dag:

    etl_task = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_etl,
    )

    notify_task = PythonOperator(
        task_id="send_telegram_notification",
        python_callable=send_notification,
        trigger_rule="all_done",
    )

    etl_task >> notify_task
