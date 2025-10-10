# -*- coding: utf-8 -*-
"""
spider.py — 台股成交值排行榜爬蟲模組（最終優化版 v2）
版本：2025-10
功能：
1. 以「每月交易日清單合併」建立穩定交易日列表
2. 自動判斷是否為交易日
3. 抓取上市、上櫃成交金額與漲跌幅資訊
4. 過濾「00」開頭證券代號
5. 雙層欄位完整性檢查（核心 + 漲跌幅）
"""

import requests
from io import StringIO
import pandas as pd
import urllib3
import time
from datetime import datetime, timedelta

urllib3.disable_warnings()

# === 🧭 1. 使用台積電月成交資料判斷交易日 ===
def get_month_trading_dates(year_month_str):
    if len(year_month_str) == 8:
        query_date = year_month_str[:6] + "01"
    else:
        query_date = year_month_str
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date={query_date}&stockNo=2330&response=json"
    trading_dates = []
    try:
        response = requests.get(url, timeout=10, verify=False)
        data = response.json()
        if data.get("stat") != "OK" or "data" not in data:
            return []
        for row in data["data"]:
            try:
                roc_year, month, day = row[0].split("/")
                year = int(roc_year) + 1911
                trading_dates.append(f"{year}{month.zfill(2)}{day.zfill(2)}")
            except Exception:
                continue
        return sorted(trading_dates, reverse=True)
    except Exception as e:
        print(f"[get_month_trading_dates] 抓取失敗：{e}")
        return []

# === 🧮 2. 以每月交易日清單合併建立 250 日清單 ===
def build_trading_dates_list(target_date_str, required_days=250):
    target_date = datetime.strptime(target_date_str, "%Y%m%d")
    all_trading_dates = []
    current_date = target_date
    max_months = 18
    while len(all_trading_dates) < required_days and max_months > 0:
        ym = current_date.strftime("%Y%m")
        month_dates = get_month_trading_dates(ym)
        if month_dates:
            valid = [d for d in month_dates if d <= target_date_str]
            all_trading_dates.extend(valid)
        if current_date.month == 1:
            current_date = current_date.replace(year=current_date.year - 1, month=12)
        else:
            current_date = current_date.replace(month=current_date.month - 1)
        max_months -= 1
        time.sleep(0.3)
    all_trading_dates = sorted(list(set(all_trading_dates)), reverse=True)
    return all_trading_dates[:required_days]

# === 📅 3. 計算各期間日期 ===
def get_period_dates(trading_dates_list, target_date_str):
    if target_date_str not in trading_dates_list:
        raise ValueError(f"{target_date_str} 不是交易日!")
    idx = trading_dates_list.index(target_date_str)
    result = {"今日": target_date_str}
    for p in [1,5,10,20,60,120,240]:
        i = idx + p
        result[f"{p}日前"] = trading_dates_list[i] if i < len(trading_dates_list) else None
    return result

# === 💹 4. 取得上市資料 ===
def get_twse_data(date):
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={date}&type=ALLBUT0999&response=csv"
    res = requests.get(url, timeout=10, verify=False)
    lines = res.text.split("\n")
    valid = [l for l in lines if len(l.split('",')) == 17]
    data_text = "\n".join(valid).replace("=", "")
    df = pd.read_csv(StringIO(data_text))
    df = df.astype(str).apply(lambda s: s.str.replace(",", ""))
    df["成交金額"] = pd.to_numeric(df["成交金額"], errors="coerce")
    df["收盤價"] = pd.to_numeric(df["收盤價"], errors="coerce")
    df = df[["證券代號", "證券名稱", "成交金額", "收盤價"]].copy()
    df["市場"] = "上市"
    df = df[~df["證券代號"].astype(str).str.startswith("00")]
    required_cols = ["證券代號","證券名稱","成交金額","收盤價","市場"]
    for c in required_cols:
        if c not in df.columns: df[c]=None
    return df

# === 💹 5. 取得上櫃資料 ===
def get_otc_data(date):
    def fmt(d): return f"{d[:4]}/{d[4:6]}/{d[6:8]}"
    res = requests.get(f"https://www.tpex.org.tw/www/zh-tw/afterTrading/otc?date={fmt(date)}&type=EW&response=csv&order=8&sort=desc",timeout=10,verify=False)
    lines = res.text.split("\n")
    valid = [l for l in lines if len(l.split(","))>10]
    data_text = "\n".join(valid).replace("=", "")
    df = pd.read_csv(StringIO(data_text))
    df = df[df["代號"].astype(str).str.len()<=4]
    df = df.astype(str).apply(lambda s:s.str.replace(",", "").str.strip())
    df.columns = df.columns.str.strip()
    df["成交金額(元)"] = pd.to_numeric(df["成交金額(元)"], errors="coerce")
    df["收盤"] = pd.to_numeric(df["收盤"], errors="coerce")
    df = df[df["成交金額(元)"]!=0]
    df = df.rename(columns={"代號":"證券代號","名稱":"證券名稱","成交金額(元)":"成交金額","收盤":"收盤價"})
    df["市場"]="上櫃"
    df = df[~df["證券代號"].astype(str).str.startswith("00")]
    required_cols = ["證券代號","證券名稱","成交金額","收盤價","市場"]
    for c in required_cols:
        if c not in df.columns: df[c]=None
    return df

# === 📊 6. 整合資料 + 計算漲跌幅 ===
def get_multi_date_data(dates_dict, market="twse"):
    f = get_twse_data if market=="twse" else get_otc_data
    all_data={}
    for label,d in dates_dict.items():
        if not d: continue
        try: all_data[label]=f(d)
        except: pass
        time.sleep(0.3)
    if "今日" not in all_data: return pd.DataFrame()
    res=all_data["今日"].copy()
    for lb in ["1日前","5日前","10日前","20日前","60日前","120日前","240日前"]:
        if lb in all_data:
            t=all_data[lb][["證券代號","收盤價"]]
            t.columns=["證券代號",f"{lb}收盤價"]
            res=res.merge(t,on="證券代號",how="left")
    for p in ["1日","5日","10日","20日","60日","120日","240日"]:
        col=f"{p}前收盤價"
        if col in res.columns:
            res[f"{p}漲跌幅"]=((res["收盤價"]-res[col])/res[col]*100).round(2)
            res.drop(columns=[col],inplace=True)
    return res

# === 🏦 7. 主函數 ===
def get_taiwan_stock_data(date, top_n=50, market="all"):
    td=build_trading_dates_list(date,250)
    dd=get_period_dates(td,date)
    twse=get_multi_date_data(dd,"twse") if market in ["all","twse"] else pd.DataFrame()
    otc=get_multi_date_data(dd,"otc") if market in ["all","otc"] else pd.DataFrame()
    combined=pd.concat([twse,otc],ignore_index=True)
    core=["證券代號","證券名稱","成交金額","收盤價","市場"]
    for c in core:
        if c not in combined.columns: combined[c]=None
    drift=["1日漲跌幅","5日漲跌幅","10日漲跌幅","20日漲跌幅","60日漲跌幅","120日漲跌幅","240日漲跌幅"]
    for c in drift:
        if c not in combined.columns: combined[c]=None
    return combined.sort_values("成交金額",ascending=False).head(top_n)

# === 🔁 8. 找出最近可用交易日 ===
def find_latest_available_date(market="all",top_n=50,max_lookback=10):
    today=datetime.today()
    for i in range(max_lookback):
        d=(today-timedelta(days=i)).strftime("%Y%m%d")
        try:
            df=get_taiwan_stock_data(d,top_n,market)
            if not df.empty: return df,d
        except Exception as e:
            print(f"[find_latest_available_date] {d} 無資料：{e}")
            continue
    raise ValueError("找不到可用的交易資料")
