import requests
from typing import List, Dict, Any


class MoexClient:
    """
    Клиент для получения цены на нефть BRENT
    через официальный MOEX ISS API.
    """

    BASE_URL = "https://iss.moex.com/iss/history/engines/futures/markets/forts/securities"

    def get_brent_history(
        self,
        ticker: str = "BRF6",
        start: str = "2024-01-01",
    ) -> List[Dict[str, Any]]:
        """
        Загрузка исторических котировок фьючерса BRENT.

        :param ticker: тикер фьючерса, например 'BRF6'
        :param start: дата начала периода в формате YYYY-MM-DD
        :return: список словарей вида:
            {
                "date": "2025-01-10",
                "open": 79.99,
                "low": 79.62,
                "high": 79.99,
                "close": 79.62,
                "settle_price": 79.99,
                "volume": 12345,
                "waprice": 79.75,
            }
        """

        url = f"{self.BASE_URL}/{ticker}.json?from={start}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()

        block = data.get("history", {})
        rows = block.get("data", [])
        columns = block.get("columns", [])

        result: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(zip(columns, row))

            result.append(
                {
                    "date": item.get("TRADEDATE"),
                    "open": item.get("OPEN"),
                    "low": item.get("LOW"),
                    "high": item.get("HIGH"),
                    "close": item.get("CLOSE"),
                    "settle_price": item.get("SETTLEPRICE"),
                    "volume": item.get("VOLUME"),
                    "waprice": item.get("WAPRICE"),
                }
            )

        return result


# Локальная проверка
if __name__ == "__main__":
    client = MoexClient()
    print(client.get_brent_history(start="2024-01-01")[:5])
