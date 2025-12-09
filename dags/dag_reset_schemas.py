from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import os


# -----------------------------------
# Подключение к Postgres
# -----------------------------------
def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", 5432),
        dbname=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )


# -----------------------------------
# Логика: Полное удаление vault и mart
# -----------------------------------
def reset_schemas():
    print("=== RESET: Removing schemas vault and mart ===")

    conn = get_conn()
    cur = conn.cursor()

    # Удаление схем с каскадом
    cur.execute("""DROP SCHEMA IF EXISTS vault CASCADE;""")
    cur.execute("""DROP SCHEMA IF EXISTS mart CASCADE;""")

    conn.commit()
    cur.close()
    conn.close()

    print("=== RESET COMPLETED SUCCESSFULLY ===")


# -----------------------------------
# DAG
# -----------------------------------
with DAG(
    dag_id="service_reset_vault_and_mart",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # запуск только вручную
    catchup=False,
    tags=["maintenance", "danger", "reset"],
    description="Полное удаление схем vault и mart (использовать только на тесте!)"
) as dag:

    PythonOperator(
        task_id="drop_vault_and_mart",
        python_callable=reset_schemas
    )
