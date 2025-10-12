import requests
from io import StringIO
import pandas as pd
import urllib3
import time
from datetime import datetime

urllib3.disable_warnings()

# 日期轉換函數
def convert_date_format(date_str):
    """YYYYMMDD → YYYY/MM/DD"""
    return f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}"

# 交易日計算
def get_month_trading_dates(year_month_str):
    """取得指定月份的所有交易日"""
    if len(year_month_str) == 6:
        query_date = year_month_str + '01'
    else:
        query_date = year_month_str
    
    url = f'https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date={query_date}&stockNo=2330&response=json'
    
    try:
        response = requests.get(url, timeout=10, verify=False)
        data = response.json()
        
        if data.get('stat') != 'OK':
            return []
        
        trading_dates = []
        for row in data.get('data', []):
            date_str = row[0]
            year, month, day = date_str.split('/')
            year = int(year) + 1911
            trading_date = f"{year}{month.zfill(2)}{day.zfill(2)}"
            trading_dates.append(trading_date)
        
        return sorted(trading_dates, reverse=True)
    except:
        return []

def build_trading_dates_list(target_date_str, required_days=250):
    """建立交易日清單"""
    target_date = datetime.strptime(target_date_str, '%Y%m%d')
    all_trading_dates = []
    current_date = target_date
    month_count = 0
    max_months = 15
    
    while len(all_trading_dates) < required_days and month_count < max_months:
        year_month = current_date.strftime('%Y%m')
        month_dates = get_month_trading_dates(year_month)
        
        if month_dates:
            valid_dates = [d for d in month_dates if d <= target_date_str]
            all_trading_dates.extend(valid_dates)
        
        if current_date.month == 1:
            current_date = current_date.replace(year=current_date.year - 1, month=12)
        else:
            current_date = current_date.replace(month=current_date.month - 1)
        
        month_count += 1
        time.sleep(0.3)
    
    return sorted(list(set(all_trading_dates)), reverse=True)

def get_period_dates(trading_dates_list, target_date_str):
    """計算各期間日期"""
    if target_date_str not in trading_dates_list:
        raise ValueError(f"{target_date_str} 不是交易日!")
    
    target_index = trading_dates_list.index(target_date_str)
    periods = [1, 5, 10, 20, 60, 120, 240]
    result = {'今日': target_date_str}
    
    for period in periods:
        past_index = target_index + period
        if past_index < len(trading_dates_list):
            result[f'{period}日前'] = trading_dates_list[past_index]
        else:
            result[f'{period}日前'] = None
    
    return result

# 資料抓取
def get_twse_data(date):
    """上市資料"""
    url = f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={date}&type=ALLBUT0999&response=csv'
    
    try:
        response = requests.get(url, timeout=10, verify=False)
    except:
        response = requests.get(url, verify=False, timeout=10)
    
    lines = response.text.split('\n')
    valid_lines = [line for line in lines if len(line.split('",')) == 17]
    data_text = "\n".join(valid_lines).replace('=', '')
    
    df = pd.read_csv(StringIO(data_text))
    df = df.astype(str).apply(lambda s: s.str.replace(',', ''))
    df = df.set_index('證券代號')
    df["成交金額"] = pd.to_numeric(df["成交金額"], errors="coerce")
    df["收盤價"] = pd.to_numeric(df["收盤價"], errors="coerce")
    
    result = df[["證券名稱", "成交金額", "收盤價"]].copy().reset_index()
    result = result[~result['證券代號'].str.startswith('00')]
    result['市場'] = '上市'
    return result

def get_otc_data(date):
    """上櫃資料"""
    formatted_date = convert_date_format(date)
    url = f'https://www.tpex.org.tw/www/zh-tw/afterTrading/otc?date={formatted_date}&type=EW&response=csv&order=8&sort=desc'
    
    try:
        response = requests.get(url, timeout=10, verify=False)
    except:
        response = requests.get(url, verify=False, timeout=10)
    
    lines = response.text.split('\n')
    valid_lines = [line for line in lines if len(line.split(',')) > 10]
    data_text = "\n".join(valid_lines).replace('=', '')
    
    df = pd.read_csv(StringIO(data_text))
    df = df[df["代號"].astype(str).str.len() <= 4]
    df = df.astype(str).apply(lambda s: s.str.replace(',', '').str.strip())
    df.columns = df.columns.str.strip()
    
    df["成交金額(元)"] = pd.to_numeric(df["成交金額(元)"], errors="coerce")
    df["收盤"] = pd.to_numeric(df["收盤"], errors="coerce")
    df = df[df["成交金額(元)"] != 0]
    
    result = df[["代號", "名稱", "成交金額(元)", "收盤"]].copy()
    result.columns = ["證券代號", "證券名稱", "成交金額", "收盤價"]
    result = result[~result['證券代號'].str.startswith('00')]
    result['市場'] = '上櫃'
    return result

# 多日資料整合
def get_multi_date_data(dates_dict, market='twse'):
    """取得多日資料並計算漲跌幅"""
    fetch_func = get_twse_data if market == 'twse' else get_otc_data
    all_data = {}
    
    for label, date_str in dates_dict.items():
        if date_str is None:
            continue
        try:
            df = fetch_func(date_str)
            all_data[label] = df
        except:
            pass
        time.sleep(0.3)
    
    if '今日' not in all_data:
        return pd.DataFrame()
    
    today_data = all_data['今日']
    result = today_data[['證券代號', '證券名稱', '成交金額', '收盤價', '市場']].copy()
    
    for label in ['1日前', '5日前', '10日前', '20日前', '60日前', '120日前', '240日前']:
        if label in all_data:
            temp = all_data[label][['證券代號', '收盤價']].copy()
            temp.columns = ['證券代號', f'{label}收盤價']
            result = result.merge(temp, on='證券代號', how='left')
    
    for period in ['1日', '5日', '10日', '20日', '60日', '120日', '240日']:
        col_name = f'{period}前收盤價'
        if col_name in result.columns:
            result[f'{period}漲跌幅'] = (
                (result['收盤價'] - result[col_name]) / result[col_name] * 100
            ).round(2)
            result.drop(columns=[col_name], inplace=True)
    
    return result

# 主函數
def get_taiwan_stock_data(date, top_n=50, market='all'):
    """
    主函數 - 保持與原版相同的函數簽名
    
    回傳欄位:
    證券代號, 證券名稱, 成交金額, 收盤價, 市場,
    1日漲跌幅, 5日漲跌幅, 10日漲跌幅, 20日漲跌幅,
    60日漲跌幅, 120日漲跌幅, 240日漲跌幅
    """
    trading_dates = build_trading_dates_list(date, required_days=250)
    dates_dict = get_period_dates(trading_dates, date)
    
    if market in ['all', 'twse']:
        twse_data = get_multi_date_data(dates_dict, market='twse')
    else:
        twse_data = pd.DataFrame()
    
    if market in ['all', 'otc']:
        otc_data = get_multi_date_data(dates_dict, market='otc')
    else:
        otc_data = pd.DataFrame()
    
    combined = pd.concat([twse_data, otc_data], ignore_index=True)
    result = combined.sort_values('成交金額', ascending=False).head(top_n)
    
    final_columns = [
        "證券代號", "證券名稱", "成交金額", "收盤價", "市場",
        "1日漲跌幅", "5日漲跌幅", "10日漲跌幅", "20日漲跌幅",
        "60日漲跌幅", "120日漲跌幅", "240日漲跌幅"
    ]
    
    for col in final_columns:
        if col not in result.columns:
            result[col] = None
    
    return result[final_columns]