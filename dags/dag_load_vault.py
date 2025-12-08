from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import json
import hashlib

import psycopg2
import boto3


# ----------------------------------------------------------
# CONFIG
# ----------------------------------------------------------

PG_HOST = "postgres"
PG_PORT = 5432
PG_DB = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_BUCKET = "currency-data"


# ----------------------------------------------------------
# HELPERS
# ----------------------------------------------------------

def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD
    )


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def md5_key(*parts) -> str:
    raw = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


# date parsers
def parse_date_ddmmyyyy_slash(s):
    return datetime.strptime(s, "%d/%m/%Y").date()


def parse_date_ddmmyyyy_dot(s):
    return datetime.strptime(s, "%d.%m.%Y").date()


def parse_date_iso(s):
    return datetime.strptime(s, "%Y-%m-%d").date()


# ----------------------------------------------------------
# CREATE TABLES (DV2.0-compliant)
# ----------------------------------------------------------

def create_vault_tables(cur):

    cur.execute("CREATE SCHEMA IF NOT EXISTS vault;")

    # HUBs
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.hub_currency (
            currency_hkey TEXT PRIMARY KEY,
            char_code TEXT NOT NULL,
            load_date TIMESTAMP NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.hub_metal (
            metal_hkey TEXT PRIMARY KEY,
            metal_code INT NOT NULL,
            load_date TIMESTAMP NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.hub_brent (
            brent_hkey TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            load_date TIMESTAMP NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.hub_date (
            date_hkey TEXT PRIMARY KEY,
            date_value DATE NOT NULL,
            load_date TIMESTAMP NOT NULL
        );
    """)

    # LINKs
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.link_currency_date (
            link_currency_date_hkey TEXT PRIMARY KEY,
            currency_hkey TEXT NOT NULL REFERENCES vault.hub_currency,
            date_hkey TEXT NOT NULL REFERENCES vault.hub_date,
            load_date TIMESTAMP NOT NULL,
            UNIQUE(currency_hkey, date_hkey)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.link_metal_date (
            link_metal_date_hkey TEXT PRIMARY KEY,
            metal_hkey TEXT NOT NULL REFERENCES vault.hub_metal,
            date_hkey TEXT NOT NULL REFERENCES vault.hub_date,
            load_date TIMESTAMP NOT NULL,
            UNIQUE(metal_hkey, date_hkey)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.link_brent_date (
            link_brent_date_hkey TEXT PRIMARY KEY,
            brent_hkey TEXT NOT NULL REFERENCES vault.hub_brent,
            date_hkey TEXT NOT NULL REFERENCES vault.hub_date,
            load_date TIMESTAMP NOT NULL,
            UNIQUE(brent_hkey, date_hkey)
        );
    """)

    # SATs (split by source = DV2.0 rule)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.sat_currency_rate (
            currency_hkey TEXT NOT NULL,
            rate_date DATE NOT NULL,
            nominal NUMERIC,
            value NUMERIC,
            load_date TIMESTAMP NOT NULL,
            record_source TEXT NOT NULL,
            UNIQUE(currency_hkey, rate_date)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.sat_metal_price (
            metal_hkey TEXT NOT NULL,
            price_date DATE NOT NULL,
            buy NUMERIC,
            sell NUMERIC,
            load_date TIMESTAMP NOT NULL,
            record_source TEXT NOT NULL,
            UNIQUE(metal_hkey, price_date)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.sat_brent_eia_price (
            brent_hkey TEXT NOT NULL,
            price_date DATE NOT NULL,
            value NUMERIC,
            load_date TIMESTAMP NOT NULL,
            record_source TEXT NOT NULL,
            UNIQUE(brent_hkey, price_date)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vault.sat_brent_moex_price (
            brent_hkey TEXT NOT NULL,
            price_date DATE NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            settle_price NUMERIC,
            volume NUMERIC,
            load_date TIMESTAMP NOT NULL,
            record_source TEXT NOT NULL,
            UNIQUE(brent_hkey, price_date)
        );
    """)


# ----------------------------------------------------------
# HUB HELPERS
# ----------------------------------------------------------

def ensure_date_hub(cur, d, load_ts):
    h = md5_key("DATE", d.isoformat())
    cur.execute("""
        INSERT INTO vault.hub_date(date_hkey, date_value, load_date)
        VALUES (%s, %s, %s)
        ON CONFLICT (date_hkey) DO NOTHING;
    """, (h, d, load_ts))
    return h


def ensure_currency_hub(cur, code, load_ts):
    code = code.upper()
    h = md5_key("CURR", code)
    cur.execute("""
        INSERT INTO vault.hub_currency(currency_hkey, char_code, load_date)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (h, code, load_ts))
    return h


def ensure_metal_hub(cur, metal_code, load_ts):
    h = md5_key("METAL", str(metal_code))
    cur.execute("""
        INSERT INTO vault.hub_metal(metal_hkey, metal_code, load_date)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (h, metal_code, load_ts))
    return h


def ensure_brent_hub(cur, source, load_ts):
    s = source.lower()
    h = md5_key("BRENT", s)
    cur.execute("""
        INSERT INTO vault.hub_brent(brent_hkey, source, load_date)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (h, s, load_ts))
    return h


# ----------------------------------------------------------
# LOADERS – CURRENCIES
# ----------------------------------------------------------

def load_currency(cur, data):

    load_ts = datetime.utcnow()
    records = data.get("records", [])
    if not records:
        return

    for r in records:
        try:
            d = parse_date_ddmmyyyy_slash(r["date"])
        except:
            continue

        h_curr = ensure_currency_hub(cur, r["char_code"], load_ts)
        h_date = ensure_date_hub(cur, d, load_ts)

        # LINK
        link_hkey = md5_key("L_CURR_DATE", h_curr, h_date)
        cur.execute("""
            INSERT INTO vault.link_currency_date(
                link_currency_date_hkey, currency_hkey, date_hkey, load_date
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (link_hkey, h_curr, h_date, load_ts))

        # SAT (no duplicates)
        cur.execute("""
            INSERT INTO vault.sat_currency_rate(
                currency_hkey, rate_date, nominal, value, load_date, record_source
            )
            VALUES (%s, %s, %s, %s, %s, 'CBR')
            ON CONFLICT DO NOTHING;
        """, (
            h_curr, d,
            r.get("nominal"), r.get("value"),
            load_ts
        ))


# ----------------------------------------------------------
# LOADERS – METALS
# ----------------------------------------------------------

def load_metals(cur, data):

    load_ts = datetime.utcnow()
    records = data.get("records", [])
    if not records:
        return

    for r in records:
        try:
            d = parse_date_ddmmyyyy_dot(r["date"])
        except:
            continue

        h_m = ensure_metal_hub(cur, r["metal_code"], load_ts)
        h_date = ensure_date_hub(cur, d, load_ts)

        link_hkey = md5_key("L_METAL_DATE", h_m, h_date)
        cur.execute("""
            INSERT INTO vault.link_metal_date(
                link_metal_date_hkey, metal_hkey, date_hkey, load_date
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (link_hkey, h_m, h_date, load_ts))

        cur.execute("""
            INSERT INTO vault.sat_metal_price(
                metal_hkey, price_date, buy, sell, load_date, record_source
            )
            VALUES (%s, %s, %s, %s, %s, 'CBR')
            ON CONFLICT DO NOTHING;
        """, (
            h_m, d,
            r.get("buy"), r.get("sell"),
            load_ts
        ))


# ----------------------------------------------------------
# LOADERS – BRENT EIA
# ----------------------------------------------------------

def load_brent_eia(cur, data):

    load_ts = datetime.utcnow()
    h_brent = ensure_brent_hub(cur, "eia", load_ts)
    records = data.get("records", [])
    if not records:
        return

    base_date = data.get("actual") or data.get("date")

    for r in records:
        d_str = r.get("period", base_date)
        try:
            d = parse_date_iso(d_str)
        except:
            continue

        h_date = ensure_date_hub(cur, d, load_ts)

        link_hkey = md5_key("L_BRENT_DATE", h_brent, h_date)
        cur.execute("""
            INSERT INTO vault.link_brent_date(
                link_brent_date_hkey, brent_hkey, date_hkey, load_date
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (link_hkey, h_brent, h_date, load_ts))

        value = None
        try:
            value = float(r.get("value"))
        except:
            pass

        cur.execute("""
            INSERT INTO vault.sat_brent_eia_price(
                brent_hkey, price_date, value, load_date, record_source
            )
            VALUES (%s, %s, %s, %s, 'EIA')
            ON CONFLICT DO NOTHING;
        """, (h_brent, d, value, load_ts))


# ----------------------------------------------------------
# LOADERS – BRENT MOEX
# ----------------------------------------------------------

def load_brent_moex(cur, data):

    load_ts = datetime.utcnow()
    h_brent = ensure_brent_hub(cur, "moex", load_ts)
    records = data.get("records", [])

    for r in records:
        d_str = r.get("date")
        try:
            d = parse_date_iso(d_str)
        except:
            continue

        h_date = ensure_date_hub(cur, d, load_ts)

        link_hkey = md5_key("L_BRENT_DATE", h_brent, h_date)
        cur.execute("""
            INSERT INTO vault.link_brent_date(
                link_brent_date_hkey, brent_hkey, date_hkey, load_date
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (link_hkey, h_brent, h_date, load_ts))

        cur.execute("""
            INSERT INTO vault.sat_brent_moex_price(
                brent_hkey, price_date,
                open, high, low, close, settle_price, volume,
                load_date, record_source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'MOEX')
            ON CONFLICT DO NOTHING;
        """, (
            h_brent, d,
            r.get("open"), r.get("high"), r.get("low"), r.get("close"),
            r.get("settle_price"), r.get("volume"),
            load_ts
        ))


# ----------------------------------------------------------
# MAIN LOAD FUNCTION
# ----------------------------------------------------------

def load_vault():

    s3 = get_s3()
    conn = get_pg_conn()
    cur = conn.cursor()

    create_vault_tables(cur)
    conn.commit()

    keys = []
    token = None

    while True:
        args = {"Bucket": MINIO_BUCKET}
        if token:
            args["ContinuationToken"] = token
        resp = s3.list_objects_v2(**args)

        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])

        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

    print(f"FOUND {len(keys)} objects")

    for key in sorted(keys):

        if not key.endswith(".json"):
            continue

        # Only historical folders:
        if not (
            key.startswith("historical/currencies/") or
            key.startswith("historical/metals/") or
            key.startswith("historical/brent_eia/") or
            key.startswith("historical/brent_moex/")
        ):
            continue

        print("Processing:", key)

        raw = s3.get_object(Bucket=MINIO_BUCKET, Key=key)["Body"].read().decode()
        try:
            data = json.loads(raw)
        except:
            print("JSON parse failed:", key)
            continue

        try:
            if key.startswith("historical/currencies/"):
                load_currency(cur, data)

            elif key.startswith("historical/metals/"):
                load_metals(cur, data)

            elif key.startswith("historical/brent_eia/"):
                load_brent_eia(cur, data)

            elif key.startswith("historical/brent_moex/"):
                load_brent_moex(cur, data)

        except Exception as e:
            print(f"ERROR {key}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print("DONE.")


# ----------------------------------------------------------
# DAG
# ----------------------------------------------------------

with DAG(
    dag_id="dag_load_vault",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 10 * * *",
    catchup=False,
    tags=["vault", "dv2"]
) as dag:

    load_task = PythonOperator(
        task_id="load_vault_from_minio",
        python_callable=load_vault
    )
