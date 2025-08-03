import requests
from io import StringIO
import pandas as pd
import urllib3

urllib3.disable_warnings()

def get_twse_data(date):
    url = f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={date}&type=ALLBUT0999&response=csv'
    try:
        response = requests.get(url, timeout=10)
    except requests.exceptions.SSLError:
        response = requests.get(url, verify=False, timeout=10)

    lines = response.text.split('\n')
    valid_lines = [line for line in lines if len(line.split('",')) == 17]
    data_text = "\n".join(valid_lines).replace('=', '')
    df = pd.read_csv(StringIO(data_text))
    df = df.astype(str).apply(lambda s: s.str.replace(',', ''))
    df = df.set_index('證券代號')
    df["成交金額"] = pd.to_numeric(df["成交金額"], errors="coerce")
    result = df[["證券名稱", "成交金額", "收盤價"]].copy().reset_index()
    result['市場'] = '上市'
    return result

def get_otc_data(date):
    url = f'https://www.tpex.org.tw/www/zh-tw/afterTrading/otc?date={date}&type=EW&response=csv&order=8&sort=desc'
    try:
        response = requests.get(url, timeout=10)
    except requests.exceptions.SSLError:
        response = requests.get(url, verify=False, timeout=10)

    lines = response.text.split('\n')
    valid_lines = [line for line in lines if len(line.split(',')) > 10]
    data_text = "\n".join(valid_lines).replace('=', '')
    df = pd.read_csv(StringIO(data_text))
    df = df[df["代號"].astype(str).str.len() <= 4]
    df = df.astype(str).apply(lambda s: s.str.replace(',', ''))
    df.columns = df.columns.str.strip()
    df["成交金額(元)"] = pd.to_numeric(df["成交金額(元)"], errors="coerce")
    df = df[df["成交金額(元)"] != 0]
    result = df[["代號", "名稱", "成交金額(元)", "收盤"]].copy()
    result.columns = ["證券代號", "證券名稱", "成交金額", "收盤價"]
    result['市場'] = '上櫃'
    return result

def get_taiwan_stock_data(date, top_n=50, market='all'):
    twse = get_twse_data(date) if market in ['twse', 'all'] else pd.DataFrame()
    otc = get_otc_data(date) if market in ['otc', 'all'] else pd.DataFrame()
    combined = pd.concat([twse, otc], ignore_index=True)
    return combined.sort_values('成交金額', ascending=False).head(top_n)
