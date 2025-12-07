import requests
import xml.etree.ElementTree as ET

class CBRClient:
    """
    Клиент для получения данных с API ЦБ РФ:
    - Курсы валют
    - Драгметаллы
    """

    URL_CURRENCIES = "https://www.cbr.ru/scripts/XML_daily.asp"
    URL_METALS = "https://www.cbr.ru/scripts/xml_metall.asp"

    @staticmethod
    def _comma_to_dot(value: str) -> float:
        """Приводит строки вида '70,2147' к float."""
        return float(value.replace(",", "."))

    def get_currency_rates(self, date: str) -> list:
        """
        Получение курсов валют за указанную дату.
        Формат даты: DD/MM/YYYY
        """
        params = {"date_req": date}
        response = requests.get(self.URL_CURRENCIES, params=params, timeout=10)
        response.raise_for_status()

        root = ET.fromstring(response.content)

        result = []
        for val in root.findall("Valute"):
            char_code = val.find("CharCode").text
            nominal = int(val.find("Nominal").text)
            value = self._comma_to_dot(val.find("Value").text)

            result.append({
                "date": date,
                "char_code": char_code,
                "nominal": nominal,
                "value": value,
            })

        return result

    def get_metals(self, date_from: str, date_to: str) -> list:
        """
        Получение котировок металлов.
        Формат даты: DD/MM/YYYY
        """
        params = {"date_req1": date_from, "date_req2": date_to}
        response = requests.get(self.URL_METALS, params=params, timeout=10)
        response.raise_for_status()

        root = ET.fromstring(response.content)

        results = []
        for rec in root.findall("Record"):
            results.append({
                "date": rec.attrib["Date"],
                "metal_code": int(rec.attrib["Code"]),
                "buy": self._comma_to_dot(rec.find("Buy").text),
                "sell": self._comma_to_dot(rec.find("Sell").text),
            })

        return results


# Локальная проверка
if __name__ == "__main__":
    cbr = CBRClient()

    print("Валюты:")
    print(cbr.get_currency_rates("01/12/2024")[:5])

    print("Металлы:")
    print(cbr.get_metals("01/07/2001", "13/07/2001")[:5])
