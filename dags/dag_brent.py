# dags/dag_brent.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.data_ingestion.moex_client import MoexClient
from src.data_ingestion.storage import save_json_to_minio


def load_brent():
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    moex = MoexClient()
    rows = moex.get_brent_history(start=yesterday)

    save_json_to_minio(rows, f"brent/{yesterday}.json")


with DAG(
    dag_id="dag_brent",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 10 * * *",
    catchup=False,
) as dag:

    PythonOperator(
        task_id="load_daily_brent",
        python_callable=load_brent,
    )
