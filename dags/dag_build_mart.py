import os
import logging
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


# -----------------------------
# Настройки подключения к Postgres
# -----------------------------
def get_postgres_conn():
    """
    Подключение к Postgres.
    Для простоты берём параметры из env (как в docker-compose),
    но при желании это можно заменить на Airflow Connection.
    """
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )
    return conn


# -----------------------------
# DDL: создание схемы и витринных таблиц
# -----------------------------
def create_mart_tables(cur):
    logging.info("Creating mart schema and tables if not exists...")

    sql = """
    CREATE SCHEMA IF NOT EXISTS mart;

    -- Dimension: calendar
    CREATE TABLE IF NOT EXISTS mart.dim_date (
        date_hkey   TEXT PRIMARY KEY,
        date_value  DATE NOT NULL
    );

    -- Dimension: currencies
    CREATE TABLE IF NOT EXISTS mart.dim_currency (
        currency_hkey TEXT PRIMARY KEY,
        char_code     TEXT NOT NULL
    );

    -- Dimension: metals
    CREATE TABLE IF NOT EXISTS mart.dim_metal (
        metal_hkey  TEXT PRIMARY KEY,
        metal_code  INTEGER NOT NULL
    );

    -- Dimension: brent sources
    CREATE TABLE IF NOT EXISTS mart.dim_brent (
        brent_hkey TEXT PRIMARY KEY,
        source     TEXT NOT NULL
    );

    -- Fact: normalized market prices
    CREATE TABLE IF NOT EXISTS mart.fact_market_prices (
        date_hkey    TEXT NOT NULL,
        entity_type  TEXT NOT NULL,   -- 'brent' | 'currency' | 'metal'
        entity_code  TEXT NOT NULL,   -- 'USD', 'EUR', 'gold', 'eia', ...
        source       TEXT NOT NULL,   -- 'cbr_currency_api', 'cbr_metals_api', 'eia', 'moex', ...
        value        NUMERIC,
        nominal      NUMERIC,
        buy          NUMERIC,
        sell         NUMERIC,
        load_date    TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
        CONSTRAINT fact_market_prices_pk
            PRIMARY KEY (date_hkey, entity_type, entity_code, source)
    );
    """

    cur.execute(sql)


# -----------------------------
# Загрузка измерений из DataVault
# -----------------------------
def load_dimensions(cur):
    logging.info("Loading mart dimensions from vault...")

    # dim_date
    cur.execute("""
        INSERT INTO mart.dim_date (date_hkey, date_value)
        SELECT d.date_hkey, d.date_value
        FROM vault.hub_date d
        ON CONFLICT (date_hkey)
        DO UPDATE SET
            date_value = EXCLUDED.date_value;
    """)

    # dim_currency
    cur.execute("""
        INSERT INTO mart.dim_currency (currency_hkey, char_code)
        SELECT c.currency_hkey, c.char_code
        FROM vault.hub_currency c
        ON CONFLICT (currency_hkey)
        DO UPDATE SET
            char_code = EXCLUDED.char_code;
    """)

    # dim_metal
    cur.execute("""
        INSERT INTO mart.dim_metal (metal_hkey, metal_code)
        SELECT m.metal_hkey, m.metal_code
        FROM vault.hub_metal m
        ON CONFLICT (metal_hkey)
        DO UPDATE SET
            metal_code = EXCLUDED.metal_code;
    """)

    # dim_brent
    cur.execute("""
        INSERT INTO mart.dim_brent (brent_hkey, source)
        SELECT b.brent_hkey, b.source
        FROM vault.hub_brent b
        ON CONFLICT (brent_hkey)
        DO UPDATE SET
            source = EXCLUDED.source;
    """)


# -----------------------------
# Загрузка фактов в витрину
# -----------------------------
def load_fact(cur):
    logging.info("Reloading mart.fact_market_prices from vault (normalize + FF for brent)...")

    # Полный пересчёт витрины — для диплома это нормальный, прозрачный подход:
    cur.execute("TRUNCATE TABLE mart.fact_market_prices;")

    # ----- CURRENCIES (без FF, дневные курсы ЦБ) -----
    logging.info("Loading currency facts...")
    cur.execute("""
        INSERT INTO mart.fact_market_prices
            (date_hkey, entity_type, entity_code, source,
             value, nominal, buy, sell, load_date)
        SELECT
            d.date_hkey,
            'currency' AS entity_type,
            c.char_code AS entity_code,
            'cbr_currency_api' AS source,
            scr.value,
            scr.nominal,
            NULL::NUMERIC AS buy,
            NULL::NUMERIC AS sell,
            scr.load_date
        FROM vault.sat_currency_rate scr
        JOIN vault.hub_currency c
             ON c.currency_hkey = scr.currency_hkey
        JOIN vault.hub_date d
             ON d.date_value = scr.rate_date;
    """)

    # ----- METALS (без FF, тоже дневные) -----
    logging.info("Loading metal facts...")
    cur.execute("""
        INSERT INTO mart.fact_market_prices
            (date_hkey, entity_type, entity_code, source,
             value, nominal, buy, sell, load_date)
        SELECT
            d.date_hkey,
            'metal' AS entity_type,
            CASE m.metal_code
                WHEN 1 THEN 'gold'
                WHEN 2 THEN 'silver'
                WHEN 3 THEN 'platinum'
                WHEN 4 THEN 'palladium'
                ELSE m.metal_code::TEXT
            END AS entity_code,
            'cbr_metals_api' AS source,
            smp.sell AS value,           -- берём цену продажи как «основную»
            NULL::NUMERIC AS nominal,
            smp.buy,
            smp.sell,
            smp.load_date
        FROM vault.sat_metal_price smp
        JOIN vault.hub_metal m
             ON m.metal_hkey = smp.metal_hkey
        JOIN vault.hub_date d
             ON d.date_value = smp.price_date;
    """)

    # ----- BRENT с forward-fill по дням и по источнику (eia / moex) -----
    logging.info("Loading brent facts with forward-fill per source...")

    brent_sql = """
        WITH raw AS (
            SELECT
                price_date::date AS date_value,
                'eia'::text      AS source,
                value::numeric   AS price
            FROM vault.sat_brent_eia_price

            UNION ALL

            SELECT
                price_date::date AS date_value,
                'moex'::text     AS source,
                value::numeric   AS price
            FROM vault.sat_brent_moex_price
        ),

        calendar_bounds AS (
            SELECT
                MIN(date_value) AS min_date,
                MAX(date_value) AS max_date
            FROM raw
        ),

        calendar AS (
            SELECT
                generate_series(min_date, max_date, interval '1 day')::date AS date_value
            FROM calendar_bounds
        ),

        sources(source) AS (
            VALUES ('eia'::text), ('moex'::text)
        ),

        grid AS (
            -- Полная решётка «все даты × оба источника»
            SELECT
                c.date_value,
                s.source
            FROM calendar c
            CROSS JOIN sources s
        ),

        numbered AS (
            -- Присоединяем к решётке реальные наблюдения и считаем "группы" ненулевых
            SELECT
                g.date_value,
                g.source,
                r.price,
                COUNT(r.price) FILTER (WHERE r.price IS NOT NULL) OVER (
                    PARTITION BY g.source
                    ORDER BY g.date_value
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS grp
            FROM grid g
            LEFT JOIN raw r
                   ON r.date_value = g.date_value
                  AND r.source = g.source
        ),

        filled AS (
            -- Для каждой группы (последовательности после первого ненулевого)
            -- берём одно и то же значение, для grp = 0 -> 0
            SELECT
                date_value,
                source,
                CASE
                    WHEN grp = 0 THEN 0::numeric
                    ELSE MAX(price) OVER (PARTITION BY source, grp)
                END AS filled_price
            FROM numbered
        ),

        with_date_hkey AS (
            SELECT
                d.date_hkey,
                f.date_value,
                f.source,
                f.filled_price
            FROM filled f
            JOIN vault.hub_date d
                 ON d.date_value = f.date_value
        )

        INSERT INTO mart.fact_market_prices
            (date_hkey, entity_type, entity_code, source,
             value, nominal, buy, sell, load_date)
        SELECT
            w.date_hkey,
            'brent' AS entity_type,
            w.source AS entity_code,   -- 'eia' или 'moex'
            w.source AS source,        -- источник = код
            w.filled_price AS value,
            NULL::NUMERIC AS nominal,
            NULL::NUMERIC AS buy,
            NULL::NUMERIC AS sell,
            now() AS load_date
        FROM with_date_hkey w;
    """

    cur.execute(brent_sql)


# -----------------------------
# Основная функция таска
# -----------------------------
def build_mart(**context):
    logging.info("===== Starting full MART rebuild =====")

    conn = get_postgres_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                create_mart_tables(cur)
                load_dimensions(cur)
                load_fact(cur)
        logging.info("===== MART rebuild finished successfully =====")
    finally:
        conn.close()


# -----------------------------
# Описание DAG
# -----------------------------
default_args = {
    "owner": "petr",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_stage_3_normalize_data_and_load_datamart",
    description="Пересборка витрины mart из Data Vault (Brent FF, currencies, metals)",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["datamart"],
) as dag:

    build_mart_task = PythonOperator(
        task_id="build_DataMart",
        python_callable=build_mart,
        provide_context=True,
    )
