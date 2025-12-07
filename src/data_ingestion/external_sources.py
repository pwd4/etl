import requests

class MoexClient:
    """
    Клиент для получения цены на нефть BRENT
    через официальный MOEX ISS API.
    """

    BASE_URL = "https://iss.moex.com/iss/history/engines/futures/markets/forts/securities"

    def get_brent_history(self, ticker: str = "BRF6", start: str = "2024-01-01") -> list:
        """
        Загрузка исторических котировок фьючерса BRENT.
        Возвращает список словарей вида:
        {
            "date": "2025-01-10",
            "close": 79.99,
            "open": 79.62,
            "high": 79.99,
            "low": 79.62,
            "settleprice": 79.99
        }
        """

        url = f"{self.BASE_URL}/{ticker}.json?from={start}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()

        block = data.get("history", {})
        rows = block.get("data", [])
        columns = block.get("columns", [])

        result = []
        for row in rows:
            item = dict(zip(columns, row))

            result.append({
                "date": item.get("TRADEDATE"),
                "open": item.get("OPEN"),
                "low": item.get("LOW"),
                "high": item.get("HIGH"),
                "close": item.get("CLOSE"),
                "settle_price": item.get("SETTLEPRICE"),
                "volume": item.get("VOLUME"),
                "waprice": item.get("WAPRICE"),
            })

        return result


# Локальная проверка
if __name__ == "__main__":
    client = MoexClient()
    print(client.get_brent_history()[:5])
