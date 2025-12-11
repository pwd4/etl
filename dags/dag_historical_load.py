from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
from botocore.exceptions import ClientError
import json

from src.data_ingestion.cbr_client import CBRClient
from src.data_ingestion.moex_client import MoexClient
from src.data_ingestion.storage import save_json_to_minio


EIA_API_KEY = "igQPBkvoO0cUVZWGzP32EzK4I36Cg0te88QLLlaB"
EIA_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"

START_DATE = "2025-01-01"
END_DATE = "2025-12-31"

BUCKET = "currency-data"


# ---------------------------
# MinIO helpers
# ---------------------------

def minio_client():
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123",
    )


def minio_has(prefix, date_iso):
    key = f"historical/{prefix}/{date_iso}.json"
    try:
        minio_client().head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError:
        return False


# ---------------------------
# Fallback logic
# ---------------------------

def fetch_with_fallback(fetch_fn, date_iso, max_back=10):
    dt = datetime.strptime(date_iso, "%Y-%m-%d")
    for back in range(max_back + 1):
        d = (dt - timedelta(days=back)).strftime("%Y-%m-%d")
        result = fetch_fn(d)
        if result:
            return result, d
    return [], None


# ---------------------------
# FETCH functions
# ---------------------------

def fetch_currency(date_iso):
    cbr = CBRClient()
    d = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    return cbr.get_currency_rates(d)


def fetch_metals(date_iso):
    cbr = CBRClient()
    d = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    return cbr.get_metals(d, d)


def fetch_brent_moex(date_iso):
    moex = MoexClient()
    return moex.get_brent_history(start=date_iso)


def fetch_brent_eia(date_iso):
    params = {
        "frequency": "daily",
        "data[0]": "value",
        "facets[series][]": "RBRTE",
        "start": date_iso,
        "end": date_iso,
        "api_key": EIA_API_KEY
    }
    try:
        r = requests.get(EIA_URL, params=params, timeout=10)
        r.raise_for_status()
        j = r.json()
        return j.get("response", {}).get("data", [])
    except:
        return []


# ---------------------------
# PARALLEL LOADERS
# ---------------------------

def load_currency_historical():
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    day = start

    while day <= end:
        date_iso = day.strftime("%Y-%m-%d")

        if not minio_has("currencies", date_iso):
            rows, actual = fetch_with_fallback(fetch_currency, date_iso)
            save_json_to_minio(
                {"target": date_iso, "actual": actual, "records": rows},
                f"historical/currencies/{date_iso}.json"
            )

        day += timedelta(days=1)


def load_metals_historical():
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    day = start

    while day <= end:
        date_iso = day.strftime("%Y-%m-%d")

        if not minio_has("metals", date_iso):
            rows, actual = fetch_with_fallback(fetch_metals, date_iso)
            save_json_to_minio(
                {"target": date_iso, "actual": actual, "records": rows},
                f"historical/metals/{date_iso}.json"
            )

        day += timedelta(days=1)


def load_brent_moex_historical():
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    day = start

    while day <= end:
        date_iso = day.strftime("%Y-%m-%d")

        if not minio_has("brent_moex", date_iso):
            rows, actual = fetch_with_fallback(fetch_brent_moex, date_iso)
            save_json_to_minio(
                {"target": date_iso, "actual": actual, "records": rows},
                f"historical/brent_moex/{date_iso}.json"
            )

        day += timedelta(days=1)


def load_brent_eia_historical():
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    day = start

    while day <= end:
        date_iso = day.strftime("%Y-%m-%d")

        if not minio_has("brent_eia", date_iso):
            rows, actual = fetch_with_fallback(fetch_brent_eia, date_iso)
            save_json_to_minio(
                {"target": date_iso, "actual": actual, "records": rows},
                f"historical/brent_eia/{date_iso}.json"
            )

        day += timedelta(days=1)


# ---------------------------
# DAG (parallel execution!)
# ---------------------------

with DAG(
    dag_id="etl_stage_1_historical_load_from_sources",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    concurrency=10,
    max_active_runs=1,
    tags=["historical"],
) as dag:

    t1 = PythonOperator(
        task_id="load_currency_historical",
        python_callable=load_currency_historical,
    )

    t2 = PythonOperator(
        task_id="load_metals_historical",
        python_callable=load_metals_historical,
    )

    t3 = PythonOperator(
        task_id="load_brent_moex_historical",
        python_callable=load_brent_moex_historical,
    )

    t4 = PythonOperator(
        task_id="load_brent_eia_historical",
        python_callable=load_brent_eia_historical,
    )

    # всё параллельно
    [t1, t2, t3, t4]
