from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.data_ingestion.cbr_client import CBRClient
from src.data_ingestion.moex_client import MoexClient
from src.data_ingestion.storage import (
    save_rows_to_postgres,
    save_json_to_minio,
)


def test_cbr():
    cbr = CBRClient()

    # одна конкретная дата для валют
    date_str = "01/02/2024"
    rates = cbr.get_currency_rates(date_str)

    # небольшой диапазон для металлов
    metals = cbr.get_metals("01/02/2024", "10/02/2024")

    print("=== Курсы валют ===")
    print(rates[:3])

    print("=== Металлы ===")
    print(metals[:3])

    # --------- Сохранение в Postgres ---------
    # таблица 1: курсы валют
    save_rows_to_postgres(
        table="cbr_currency_rates",
        rows=rates,
        columns=["date", "char_code", "nominal", "value"],
        primary_key=None,  # пока без PK, чтобы не усложнять
    )

    # таблица 2: металлы
    save_rows_to_postgres(
        table="cbr_metals",
        rows=metals,
        columns=["date", "metal_code", "buy", "sell"],
        primary_key=None,
    )

    # --------- Сохранение в MinIO ---------
    save_json_to_minio(rates, "test/cbr_rates_2024-12-01.json")
    save_json_to_minio(metals, "test/cbr_metals_2024-01-01_2024-01-05.json")


def test_moex():
    moex = MoexClient()
    start_date = "2024-02-01"
    rows = moex.get_brent_history(start=start_date)

    print("=== Нефть BRENT ===")
    print(rows[:3])

    if not rows:
        return

    # ключи должны совпадать с тем, что возвращает MoexClient
    columns = [
        "date",
        "open",
        "low",
        "high",
        "close",
        "settle_price",
        "volume",
        "waprice",
    ]

    # --------- Postgres ---------
    save_rows_to_postgres(
        table="moex_brent",
        rows=rows,
        columns=columns,
        primary_key=None,
    )

    # --------- MinIO ---------
    save_json_to_minio(rows, f"test/brent_from_{start_date}.json")


with DAG(
    dag_id="test_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="test_cbr",
        python_callable=test_cbr,
    )

    t2 = PythonOperator(
        task_id="test_moex",
        python_callable=test_moex,
    )

    t1 >> t2
