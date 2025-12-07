import json
from typing import List, Dict, Any

import psycopg2
from psycopg2.extras import execute_batch
import boto3
from botocore.exceptions import ClientError


# ---------- POSTGRES ----------

PG_HOST = "postgres"
PG_PORT = 5432
PG_DB = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"


def _pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )


def save_rows_to_postgres(
    table: str,
    rows: List[Dict[str, Any]],
    columns: List[str],
    primary_key: str = None
):
    """
    Универсальная запись данных в Postgres.
    Создаёт таблицу, если её нет.
    Обновляет строки по первичному ключу.
    """

    if not rows:
        return

    conn = _pg_conn()
    cur = conn.cursor()

    # Создаём таблицу
    cols_sql = ",\n".join([
        f"{c} text" if c != primary_key else f"{c} text PRIMARY KEY"
        for c in columns
    ])

    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {cols_sql}
        );
        """
    )

    values = [
        tuple(str(r[col]) for col in columns)
        for r in rows
    ]

    placeholders = ",".join(["%s"] * len(columns))
    conflict_sql = "" if not primary_key else \
        f"ON CONFLICT ({primary_key}) DO UPDATE SET " + \
        ", ".join([f"{c}=EXCLUDED.{c}" for c in columns if c != primary_key])

    execute_batch(
        cur,
        f"""
        INSERT INTO {table} ({",".join(columns)})
        VALUES ({placeholders})
        {conflict_sql};
        """,
        values,
    )

    conn.commit()
    conn.close()


# ---------- MINIO ----------

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_BUCKET = "currency-data"


def _minio():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def ensure_bucket(bucket: str = MINIO_BUCKET):
    client = _minio()
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)


def save_json_to_minio(obj: Any, key: str, bucket: str = MINIO_BUCKET):
    ensure_bucket(bucket)
    client = _minio()
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(obj, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
