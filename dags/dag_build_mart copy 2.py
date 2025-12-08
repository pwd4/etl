from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


# =====================================================================
# Подключение к БД
# =====================================================================
def get_conn():
    return psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow",
    )


# =====================================================================
# Создание схемы и таблиц mart
# =====================================================================
def create_mart_tables(cur):
    # Схема mart
    cur.execute("CREATE SCHEMA IF NOT EXISTS mart;")

    # Сначала дропаем факт, потом измерения
    cur.execute("""
        DROP TABLE IF EXISTS mart.fact_market_prices;
        DROP TABLE IF EXISTS mart.dim_brent;
        DROP TABLE IF EXISTS mart.dim_currency;
        DROP TABLE IF EXISTS mart.dim_metal;
        DROP TABLE IF EXISTS mart.dim_date;
    """)

    # dim_date
    cur.execute("""
        CREATE TABLE mart.dim_date (
            date_hkey   TEXT PRIMARY KEY,
            date_value  DATE NOT NULL
        );
    """)

    # dim_brent
    cur.execute("""
        CREATE TABLE mart.dim_brent (
            brent_hkey  TEXT PRIMARY KEY,
            source      TEXT NOT NULL
        );
    """)

    # dim_currency
    cur.execute("""
        CREATE TABLE mart.dim_currency (
            currency_hkey  TEXT PRIMARY KEY,
            char_code      TEXT NOT NULL
        );
    """)

    # dim_metal
    cur.execute("""
        CREATE TABLE mart.dim_metal (
            metal_hkey  TEXT PRIMARY KEY,
            metal_code  INTEGER NOT NULL,
            metal_name  TEXT
        );
    """)

    # Факт — одна общая таблица под все типы сущностей
    cur.execute("""
        CREATE TABLE mart.fact_market_prices (
            date_hkey    TEXT NOT NULL,
            entity_type  TEXT NOT NULL,   -- 'brent' / 'currency' / 'metal'
            entity_code  TEXT NOT NULL,   -- 'eia', 'USD', '1' и т.п.
            source       TEXT NOT NULL,   -- 'eia', 'moex', 'cbr_currency_api', 'cbr_metals_api'
            value        NUMERIC,         -- курс / цена (brent / currency)
            nominal      NUMERIC,         -- номинал валюты (для CBR)
            buy          NUMERIC,         -- цены покупки металлов
            sell         NUMERIC,         -- цены продажи металлов
            load_date    TIMESTAMP NOT NULL
        );
    """)


# =====================================================================
# Загрузка измерений из vault
# =====================================================================
def load_dims(cur):
    # dim_date — просто копия hub_date
    cur.execute("""
        INSERT INTO mart.dim_date (date_hkey, date_value)
        SELECT
            hd.date_hkey,
            hd.date_value
        FROM vault.hub_date hd;
    """)

    # dim_brent
    cur.execute("""
        INSERT INTO mart.dim_brent (brent_hkey, source)
        SELECT
            hb.brent_hkey,
            hb.source
        FROM vault.hub_brent hb;
    """)

    # dim_currency
    cur.execute("""
        INSERT INTO mart.dim_currency (currency_hkey, char_code)
        SELECT
            hc.currency_hkey,
            hc.char_code
        FROM vault.hub_currency hc;
    """)

    # dim_metal + маппинг кода в название
    cur.execute("""
        INSERT INTO mart.dim_metal (metal_hkey, metal_code, metal_name)
        SELECT
            hm.metal_hkey,
            hm.metal_code,
            CASE hm.metal_code
                WHEN 1 THEN 'gold'
                WHEN 2 THEN 'silver'
                WHEN 3 THEN 'platinum'
                WHEN 4 THEN 'palladium'
                ELSE 'unknown'
            END AS metal_name
        FROM vault.hub_metal hm;
    """)


# =====================================================================
# Загрузка факта: БЕЗ заполнения пропусков, только реальные даты
# =====================================================================
def load_fact(cur):
    # ---------- BRENT: EIA ----------
    cur.execute("""
        INSERT INTO mart.fact_market_prices (
            date_hkey,
            entity_type,
            entity_code,
            source,
            value,
            nominal,
            buy,
            sell,
            load_date
        )
        SELECT
            lbd.date_hkey                              AS date_hkey,
            'brent'                                    AS entity_type,
            hb.source                                  AS entity_code,   -- 'eia'
            hb.source                                  AS source,
            s.value                                    AS value,
            NULL::NUMERIC                              AS nominal,
            NULL::NUMERIC                              AS buy,
            NULL::NUMERIC                              AS sell,
            s.load_date                                AS load_date
        FROM vault.hub_brent hb
        JOIN vault.link_brent_date lbd
          ON lbd.brent_hkey = hb.brent_hkey
        JOIN vault.hub_date d
          ON d.date_hkey = lbd.date_hkey
        JOIN vault.sat_brent_eia_price s
          ON s.brent_hkey = hb.brent_hkey
         AND s.price_date = d.date_value
        WHERE hb.source = 'eia';
    """)

    # ---------- BRENT: MOEX ----------
    cur.execute("""
        INSERT INTO mart.fact_market_prices (
            date_hkey,
            entity_type,
            entity_code,
            source,
            value,
            nominal,
            buy,
            sell,
            load_date
        )
        SELECT
            lbd.date_hkey                              AS date_hkey,
            'brent'                                    AS entity_type,
            hb.source                                  AS entity_code,   -- 'moex'
            hb.source                                  AS source,
            s.value                                    AS value,
            NULL::NUMERIC                              AS nominal,
            NULL::NUMERIC                              AS buy,
            NULL::NUMERIC                              AS sell,
            s.load_date                                AS load_date
        FROM vault.hub_brent hb
        JOIN vault.link_brent_date lbd
          ON lbd.brent_hkey = hb.brent_hkey
        JOIN vault.hub_date d
          ON d.date_hkey = lbd.date_hkey
        JOIN vault.sat_brent_moex_price s
          ON s.brent_hkey = hb.brent_hkey
         AND s.price_date = d.date_value
        WHERE hb.source = 'moex';
    """)

    # ---------- CURRENCY (CBR) ----------
    cur.execute("""
        INSERT INTO mart.fact_market_prices (
            date_hkey,
            entity_type,
            entity_code,
            source,
            value,
            nominal,
            buy,
            sell,
            load_date
        )
        SELECT
            lcd.date_hkey                              AS date_hkey,
            'currency'                                 AS entity_type,
            hc.char_code                               AS entity_code,   -- 'USD', 'EUR', ...
            'cbr_currency_api'                         AS source,
            scr.value                                  AS value,
            scr.nominal                                AS nominal,
            NULL::NUMERIC                              AS buy,
            NULL::NUMERIC                              AS sell,
            scr.load_date                              AS load_date
        FROM vault.hub_currency hc
        JOIN vault.link_currency_date lcd
          ON lcd.currency_hkey = hc.currency_hkey
        JOIN vault.hub_date d
          ON d.date_hkey = lcd.date_hkey
        JOIN vault.sat_currency_rate scr
          ON scr.currency_hkey = hc.currency_hkey
         AND scr.rate_date = d.date_value;
    """)

    # ---------- METALS (CBR) ----------
    cur.execute("""
        INSERT INTO mart.fact_market_prices (
            date_hkey,
            entity_type,
            entity_code,
            source,
            value,
            nominal,
            buy,
            sell,
            load_date
        )
        SELECT
            lmd.date_hkey                              AS date_hkey,
            'metal'                                    AS entity_type,
            hm.metal_code::TEXT                        AS entity_code,   -- '1','2','3','4'
            'cbr_metals_api'                           AS source,
            NULL::NUMERIC                              AS value,
            NULL::NUMERIC                              AS nominal,
            smp.buy                                    AS buy,
            smp.sell                                   AS sell,
            smp.load_date                              AS load_date
        FROM vault.hub_metal hm
        JOIN vault.link_metal_date lmd
          ON lmd.metal_hkey = hm.metal_hkey
        JOIN vault.hub_date d
          ON d.date_hkey = lmd.date_hkey
        JOIN vault.sat_metal_price smp
          ON smp.metal_hkey = hm.metal_hkey
         AND smp.price_date = d.date_value;
    """)


# =====================================================================
# Основная функция сборки витрины
# =====================================================================
def build_mart():
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                create_mart_tables(cur)
                load_dims(cur)
                load_fact(cur)
    finally:
        conn.close()


# =====================================================================
# DAG
# =====================================================================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

with DAG(
    dag_id="dag_build_mart",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # запускаешь руками через UI
    catchup=False,
    default_args=default_args,
) as dag:

    build_mart_task = PythonOperator(
        task_id="build_mart",
        python_callable=build_mart,
    )
