import requests
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
from minio_uploader import upload_to_minio

def fetch_futures(security: str, start_date: str, end_date: str):
    url = f"https://iss.moex.com/iss/history/engines/futures/markets/forts/securities/{security}.json"
    params = {
        'from': start_date,
        'till': end_date,
        'limit': 1000
    }
    
    all_data = []
    start = 0
    
    while True:
        params['start'] = start
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        history = data.get('history', {})
        columns = history.get('columns', [])
        rows = history.get('data', [])
        
        if not rows:
            break
        
        for row in rows:
            row_dict = dict(zip(columns, row))
            row_dict['SECID'] = security
            all_data.append(row_dict)
        
        start += len(rows)
    
    df = pd.DataFrame(all_data)
    return df

def historical_load():
    securities = ['Si', 'Eu', 'BR']
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = '2018-01-01'
    
    all_dfs = []
    for sec in securities:
        print(f"Fetching {sec}...")
        df = fetch_futures(sec, start_date, end_date)
        if df is not None and not df.empty:
            all_dfs.append(df)
    
    if all_dfs:
        result_df = pd.concat(all_dfs, ignore_index=True)
        csv_buffer = StringIO()
        result_df.to_csv(csv_buffer, index=False)
        upload_to_minio(
            data=csv_buffer.getvalue(),
            bucket_name='currency-data-lake',
            object_name=f'raw/moex_futures_{start_date}_{end_date}.csv'
        )
        return result_df
    return None