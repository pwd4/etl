from datetime import datetime, timedelta

from data_ingestion.cbr_client import CBRClient
from data_ingestion.moex_client import MoexClient
from data_ingestion.storage import (
    save_rows_to_postgres,
    save_json_to_minio,
)


# ----------- CURRENCIES -----------

def load_currencies_for_date(date_iso: str):
    """
    Загружает курсы валют ЦБ РФ за одну дату.
    date_iso: 'YYYY-MM-DD'
    """

    dt = datetime.strptime(date_iso, "%Y-%m-%d")
    cbr_date = dt.strftime("%d/%m/%Y")

    client = CBRClient()
    rows = client.get_currency_rates(cbr_date)

    save_rows_to_postgres(
        table="currency_rates",
        rows=rows,
        columns=["date", "char_code", "nominal", "value"],
        primary_key="date"
    )

    save_json_to_minio(rows, key=f"currencies/{date_iso}.json")

    return rows


# ----------- METALS -----------

def load_metals_for_period(date_iso: str):
    """
    Загружает котировки металлов за один день (в API нужен диапазон).
    """

    dt = datetime.strptime(date_iso, "%Y-%m-%d")
    d1 = dt.strftime("%d/%m/%Y")
    d2 = dt.strftime("%d/%m/%Y")

    client = CBRClient()
    rows = client.get_metals(d1, d2)

    save_rows_to_postgres(
        table="metal_quotes",
        rows=rows,
        columns=["date", "metal_code", "buy", "sell"],
        primary_key="date"
    )

    save_json_to_minio(rows, key=f"metals/{date_iso}.json")

    return rows


# ----------- BRENT -----------

def load_brent_history(start: str):
    """
    Загружает BRENT начиная с указанной даты YYYY-MM-DD
    """

    client = MoexClient()
    rows = client.get_brent_history(start=start)

    save_rows_to_postgres(
        table="brent_quotes",
        rows=rows,
        columns=[
            "date", "open", "low", "high", "close",
            "settle_price", "volume", "waprice"
        ],
        primary_key="date"
    )

    save_json_to_minio(rows, key=f"brent/{start}.json")

    return rows
