import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
from minio_uploader import upload_to_minio

def fetch_currency(date: datetime):
    url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={date.strftime('%d/%m/%Y')}"
    response = requests.get(url)
    response.raise_for_status()
    
    root = ET.fromstring(response.content)
    data = []
    for valute in root.findall('Valute'):
        charcode = valute.find('CharCode').text
        value = float(valute.find('Value').text.replace(',', '.'))
        nominal = int(valute.find('Nominal').text)
        rate = value / nominal
        
        data.append({
            'date': date.strftime('%Y-%m-%d'),
            'currency_code': charcode,
            'rate': rate
        })
    
    df = pd.DataFrame(data)
    return df

def historical_load(start_date='2018-01-01', end_date=None):
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    all_data = []
    while current_date <= end_date:
        try:
            df = fetch_currency(current_date)
            all_data.append(df)
            print(f"Fetched {current_date.strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"Error for {current_date}: {e}")
        current_date += timedelta(days=1)
    
    if all_data:
        result_df = pd.concat(all_data, ignore_index=True)
        csv_buffer = StringIO()
        result_df.to_csv(csv_buffer, index=False)
        upload_to_minio(
            data=csv_buffer.getvalue(),
            bucket_name='currency-data-lake',
            object_name=f'raw/cbr_currency_{start_date}_{end_date}.csv'
        )
        return result_df
    return None