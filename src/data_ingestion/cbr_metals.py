import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
from minio_uploader import upload_to_minio

def fetch_metals(start_date: datetime, end_date: datetime):
    url = f"https://www.cbr.ru/scripts/xml_metall.asp?date_req1={start_date.strftime('%d/%m/%Y')}&date_req2={end_date.strftime('%d/%m/%Y')}"
    response = requests.get(url)
    response.raise_for_status()
    
    root = ET.fromstring(response.content)
    code_to_metal = {
        '1': 'gold',
        '2': 'silver',
        '3': 'platinum',
        '4': 'palladium'
    }
    
    data = []
    for record in root.findall('Record'):
        date_str = record.get('Date')
        date = datetime.strptime(date_str, '%d.%m.%Y').strftime('%Y-%m-%d')
        code = record.get('Code')
        metal = code_to_metal.get(code)
        buy = float(record.find('Buy').text.replace(',', '.'))
        sell = float(record.find('Sell').text.replace(',', '.'))
        
        data.append({
            'date': date,
            'metal': metal,
            'buy_price': buy,
            'sell_price': sell
        })
    
    df = pd.DataFrame(data)
    return df

def historical_load(start_date='2018-01-01', end_date=None):
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    df = fetch_metals(start_dt, end_dt)
    
    if df is not None and not df.empty:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        upload_to_minio(
            data=csv_buffer.getvalue(),
            bucket_name='currency-data-lake',
            object_name=f'raw/cbr_metals_{start_date}_{end_date}.csv'
        )
        return df
    return None