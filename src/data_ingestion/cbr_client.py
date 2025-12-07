import requests
import xml.etree.ElementTree as ET
from typing import List, Dict, Any


class CBRClient:
    """
    Клиент для получения данных с API ЦБ РФ:
    - Курсы валют (XML_daily.asp)
    - Драгметаллы (xml_metall.asp)
    """

    URL_CURRENCIES = "https://www.cbr.ru/scripts/XML_daily.asp"
    URL_METALS = "https://www.cbr.ru/scripts/xml_metall.asp"

    @staticmethod
    def _comma_to_dot(value: str) -> float:
        """Приводит строки вида '70,2147' к float."""
        return float(value.replace(",", "."))

    def get_currency_rates(self, date: str) -> List[Dict[str, Any]]:
        """
        Получение курсов валют за указанную дату.
        Формат даты: DD/MM/YYYY (как ждёт ЦБ РФ).
        """
        params = {"date_req": date}
        response = requests.get(self.URL_CURRENCIES, params=params, timeout=10)
        response.raise_for_status()

        root = ET.fromstring(response.content)

        result: List[Dict[str, Any]] = []
        for val in root.findall("Valute"):
            char_code = val.find("CharCode").text
            nominal = int(val.find("Nominal").text)
            value = self._comma_to_dot(val.find("Value").text)

            result.append(
                {
                    "date": date,           # строка DD/MM/YYYY
                    "char_code": char_code, # код валюты, например 'USD'
                    "nominal": nominal,     # номинал, например 1 или 10
                    "value": value,         # курс в рублях за nominal
                }
            )

        return result

    def get_metals(self, date_from: str, date_to: str) -> List[Dict[str, Any]]:
        """
        Получение котировок металлов за период.
        Формат дат: DD/MM/YYYY
        """
        params = {"date_req1": date_from, "date_req2": date_to}
        response = requests.get(self.URL_METALS, params=params, timeout=10)
        response.raise_for_status()

        root = ET.fromstring(response.content)

        results: List[Dict[str, Any]] = []
        for rec in root.findall("Record"):
            results.append(
                {
                    "date": rec.attrib["Date"],              # DD/MM/YYYY
                    "metal_code": int(rec.attrib["Code"]),   # код металла
                    "buy": self._comma_to_dot(rec.find("Buy").text),
                    "sell": self._comma_to_dot(rec.find("Sell").text),
                }
            )

        return results


# Локальная проверка
if __name__ == "__main__":
    cbr = CBRClient()

    print("Валюты:")
    print(cbr.get_currency_rates("01/12/2024")[:5])

    print("Металлы:")
    print(cbr.get_metals("01/07/2001", "13/07/2001")[:5])
