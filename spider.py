# -*- coding: utf-8 -*-
"""
spider_optimized_v4.py — 台股成交值排行榜爬蟲模組（極簡啟動版）

版本：2025-10-12
特色：
1. 啟動時「完全不執行」任何耗時操作
2. 只提供健康檢查端點,保證Render快速啟動
3. 所有複雜運算都在「使用者請求時」才執行
4. 延遲初始化(Lazy Initialization)設計模式
"""

import requests
from io import StringIO
import pandas as pd
import urllib3
import time
from datetime import datetime, timedelta

urllib3.disable_warnings()

# ========== 全域快取變數(啟動時保持空白,不做任何處理) ==========
TRADING_DATES_CACHE = None
CACHE_DATE = None


# ========== 1. 取得指定月份的所有交易日 ==========
def get_month_trading_dates(year_month_str):
    """
    從證交所抓取某月份的交易日清單
    
    說明：
    這個函數「不會」在啟動時執行,只有當使用者呼叫API時才會啟動。
    透過台積電(2330)的月交易資料來判斷哪些日期是交易日。
    """

        # 修改為
    if len(year_month_str) == 6:
        query_date = year_month_str + "01"  # 202510 → 20251001
    elif len(year_month_str) == 8:
        query_date = year_month_str[:6] + "01"  # 20251012 → 20251001
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
        print(f"[錯誤] get_month_trading_dates: {e}")
        return []


# ========== 2. 建立完整的交易日清單 ==========
def build_trading_dates_list(target_date_str, required_days=250):
    """
    建立從指定日期往前推的250個交易日清單
    
    說明：
    這個函數「不會」在啟動時執行。
    只有當第一次呼叫get_taiwan_stock_data()時才會執行。
    執行時會向證交所發送約18次請求,耗時約25秒。
    """
    target_date = datetime.strptime(target_date_str, "%Y%m%d")
    all_trading_dates = []
    current_date = target_date
    max_months = 18
    
    print(f"[交易日清單] 開始建立,目標日期: {target_date_str}")
    
    while len(all_trading_dates) < required_days and max_months > 0:
        year_month = current_date.strftime("%Y%m")
        month_dates = get_month_trading_dates(year_month)
        
        if month_dates:
            valid_dates = [d for d in month_dates if d <= target_date_str]
            all_trading_dates.extend(valid_dates)
            print(f"[交易日清單] {year_month} 月取得 {len(valid_dates)} 個交易日")
        
        if current_date.month == 1:
            current_date = current_date.replace(year=current_date.year - 1, month=12)
        else:
            current_date = current_date.replace(month=current_date.month - 1)
        
        max_months -= 1
        time.sleep(0.3)
    
    all_trading_dates = sorted(list(set(all_trading_dates)), reverse=True)
    print(f"[交易日清單] 完成! 共 {len(all_trading_dates[:required_days])} 個交易日")
    
    return all_trading_dates[:required_days]


# ========== 3. 計算各期間的對應日期 ==========
def get_period_dates(trading_dates_list, target_date_str):
    """計算今日及過去1/5/10/20/60/120/240個交易日前的日期"""
    if target_date_str not in trading_dates_list:
        raise ValueError(f"{target_date_str} 不是交易日!")
    
    target_index = trading_dates_list.index(target_date_str)
    periods = [1, 5, 10, 20, 60, 120, 240]
    result = {"今日": target_date_str}
    
    for period in periods:
        past_index = target_index + period
        if past_index < len(trading_dates_list):
            result[f"{period}日前"] = trading_dates_list[past_index]
        else:
            result[f"{period}日前"] = None
    
    return result


# ========== 4. 抓取上市股票資料 ==========
def get_twse_data(date):
    """從證交所抓取上市股票資料"""
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={date}&type=ALLBUT0999&response=csv"
    
    response = requests.get(url, timeout=10, verify=False)
    lines = response.text.split("\n")
    valid_lines = [line for line in lines if len(line.split('",')) == 17]
    data_text = "\n".join(valid_lines).replace("=", "")
    
    df = pd.read_csv(StringIO(data_text))
    df = df.astype(str).apply(lambda s: s.str.replace(",", ""))
    df["成交金額"] = pd.to_numeric(df["成交金額"], errors="coerce")
    df["收盤價"] = pd.to_numeric(df["收盤價"], errors="coerce")
    
    df = df[["證券代號", "證券名稱", "成交金額", "收盤價"]].copy()
    df["市場"] = "上市"
    df = df[~df["證券代號"].astype(str).str.startswith("00")]
    
    return df


# ========== 5. 抓取上櫃股票資料 ==========
def get_otc_data(date):
    """從櫃買中心抓取上櫃股票資料"""
    def format_date(d):
        return f"{d[:4]}/{d[4:6]}/{d[6:8]}"
    
    formatted_date = format_date(date)
    url = f"https://www.tpex.org.tw/www/zh-tw/afterTrading/otc?date={formatted_date}&type=EW&response=csv&order=8&sort=desc"
    
    response = requests.get(url, timeout=10, verify=False)
    lines = response.text.split("\n")
    valid_lines = [line for line in lines if len(line.split(",")) > 10]
    data_text = "\n".join(valid_lines).replace("=", "")
    
    df = pd.read_csv(StringIO(data_text))
    df = df[df["代號"].astype(str).str.len() <= 4]
    df = df.astype(str).apply(lambda s: s.str.replace(",", "").str.strip())
    df.columns = df.columns.str.strip()
    
    df["成交金額(元)"] = pd.to_numeric(df["成交金額(元)"], errors="coerce")
    df["收盤"] = pd.to_numeric(df["收盤"], errors="coerce")
    df = df[df["成交金額(元)"] != 0]
    
    df = df.rename(columns={
        "代號": "證券代號",
        "名稱": "證券名稱",
        "成交金額(元)": "成交金額",
        "收盤": "收盤價"
    })
    df["市場"] = "上櫃"
    df = df[~df["證券代號"].astype(str).str.startswith("00")]
    
    return df


# ========== 6. 整合多日資料並計算漲跌幅 ==========
def get_multi_date_data(dates_dict, market="twse"):
    """抓取多日資料並計算漲跌幅"""
    fetch_func = get_twse_data if market == "twse" else get_otc_data
    all_data = {}
    
    for label, date_str in dates_dict.items():
        if date_str is None:
            continue
        try:
            all_data[label] = fetch_func(date_str)
            print(f"[資料抓取] {market} {label} ({date_str}) 成功")
        except Exception as e:
            print(f"[資料抓取] {market} {label} ({date_str}) 失敗: {e}")
            pass
        time.sleep(0.3)
    
    if "今日" not in all_data:
        return pd.DataFrame()
    
    result = all_data["今日"].copy()
    
    for label in ["1日前", "5日前", "10日前", "20日前", "60日前", "120日前", "240日前"]:
        if label in all_data:
            temp = all_data[label][["證券代號", "收盤價"]].copy()
            temp.columns = ["證券代號", f"{label}收盤價"]
            result = result.merge(temp, on="證券代號", how="left")
    
    for period in ["1日", "5日", "10日", "20日", "60日", "120日", "240日"]:
        col_name = f"{period}前收盤價"
        if col_name in result.columns:
            result[f"{period}漲跌幅"] = (
                (result["收盤價"] - result[col_name]) / result[col_name] * 100
            ).round(2)
            result.drop(columns=[col_name], inplace=True)
    
    return result


# ========== 7. 主函數(延遲初始化設計) ==========
def get_taiwan_stock_data(date, top_n=50, market="all"):
    """
    台股成交值排行榜查詢主函數
    
    延遲初始化設計：
    - 啟動時此函數「不會被呼叫」
    - 只有當N8N發送請求時才會執行
    - 第一次執行會建立交易日快取(耗時25秒)
    - 後續相同日期的查詢會使用快取(耗時3秒)
    
    輸入參數：
    - date: 查詢日期,格式 "YYYYMMDD"
    - top_n: 取前幾名,預設50
    - market: "all"(全部)、"twse"(上市)、"otc"(上櫃)
    
    回傳結果：
    - DataFrame,包含證券代號、名稱、成交金額、收盤價、市場、
              1日漲跌幅、5日漲跌幅、10日漲跌幅、20日漲跌幅、
              60日漲跌幅、120日漲跌幅、240日漲跌幅
    """
    global TRADING_DATES_CACHE, CACHE_DATE
    
    print(f"\n{'='*60}")
    print(f"[主函數] 開始執行")
    print(f"查詢日期: {date} | Top N: {top_n} | 市場: {market}")
    print(f"{'='*60}\n")
    
    # 延遲初始化:只有在需要時才建立快取
    if TRADING_DATES_CACHE is None or CACHE_DATE != date:
        print("[主函數] 快取不存在,開始建立交易日清單...")
        print("[主函數] 這個步驟需要約25秒,請稍候...\n")
        TRADING_DATES_CACHE = build_trading_dates_list(date, required_days=250)
        CACHE_DATE = date
        print(f"\n[主函數] 快取建立完成,共 {len(TRADING_DATES_CACHE)} 個交易日\n")
    else:
        print(f"[主函數] 使用已建立的快取({len(TRADING_DATES_CACHE)} 個交易日)\n")
    
    dates_dict = get_period_dates(TRADING_DATES_CACHE, date)
    print(f"[主函數] 期間日期:")
    for label, d in dates_dict.items():
        print(f"  {label}: {d}")
    print()
    
    if market in ["all", "twse"]:
        print("[主函數] 開始抓取上市資料...")
        twse_data = get_multi_date_data(dates_dict, market="twse")
        print(f"[主函數] 上市資料完成,共 {len(twse_data)} 檔股票\n")
    else:
        twse_data = pd.DataFrame()
    
    if market in ["all", "otc"]:
        print("[主函數] 開始抓取上櫃資料...")
        otc_data = get_multi_date_data(dates_dict, market="otc")
        print(f"[主函數] 上櫃資料完成,共 {len(otc_data)} 檔股票\n")
    else:
        otc_data = pd.DataFrame()
    
    combined = pd.concat([twse_data, otc_data], ignore_index=True)
    
    core_cols = ["證券代號", "證券名稱", "成交金額", "收盤價", "市場"]
    drift_cols = ["1日漲跌幅", "5日漲跌幅", "10日漲跌幅", "20日漲跌幅", 
                  "60日漲跌幅", "120日漲跌幅", "240日漲跌幅"]
    
    for col in core_cols + drift_cols:
        if col not in combined.columns:
            combined[col] = None
    
    result = combined.sort_values("成交金額", ascending=False).head(top_n)
    
    print(f"[主函數] 執行完成! 回傳 Top {top_n} 股票")
    print(f"{'='*60}\n")
    
    return result[core_cols + drift_cols]


# ========== 8. 自動尋找最近可用交易日 ==========
def find_latest_available_date(market="all", top_n=50, max_lookback=10):
    """
    自動往前尋找最近的可用交易日
    
    說明：
    這個函數也「不會」在啟動時執行。
    適用於不確定今天是否為交易日的情況。
    """
    today = datetime.today()
    
    print(f"[自動尋找] 開始尋找最近可用交易日...")
    
    for i in range(max_lookback):
        check_date = (today - timedelta(days=i)).strftime("%Y%m%d")
        
        try:
            print(f"[自動尋找] 嘗試 {check_date}...")
            df = get_taiwan_stock_data(check_date, top_n, market)
            
            if not df.empty:
                print(f"[自動尋找] 成功! 使用日期: {check_date}\n")
                return df, check_date
                
        except Exception as e:
            print(f"[自動尋找] {check_date} 無資料: {e}")
            continue
    
    raise ValueError(f"往前找了 {max_lookback} 天都找不到可用的交易資料")


# ========== 9. 健康檢查函數(給Render使用) ==========
def health_check():
    """
    健康檢查函數 - 給Render的Health Check端點使用
    
    重要：
    這個函數「非常簡單」,只回傳一個固定訊息。
    不會執行任何耗時操作,確保Render可以在1秒內得到回應。
    這是極簡啟動設計的核心概念。
    """
    return {
        "status": "healthy",
        "service": "Taiwan Stock API",
        "message": "服務正常運作",
        "cache_loaded": TRADING_DATES_CACHE is not None,
        "cache_date": CACHE_DATE
    }



# ========== 10. 測試程式碼(僅本地執行) ==========
if __name__ == "__main__":
    """
    本地測試用程式碼
    
    重要：
    這段程式碼「不會」在Render部署時執行。
    只有在本地用 python spider_optimized_v4.py 執行時才會跑。
    """
    print("\n" + "="*60)
    print("本地測試模式")
    print("="*60 + "\n")
    
    # 測試健康檢查(模擬Render的行為)
    print("測試1: 健康檢查")
    print("-" * 60)
    health_status = health_check()
    print(f"健康狀態: {health_status}")
    print("\n")
    
    # 測試資料查詢
    print("測試2: 查詢股票資料")
    print("-" * 60)
    test_date = "20251009"
    df = get_taiwan_stock_data(date=test_date, top_n=5, market="all")
    print("\n查詢結果(前5名):")
    print(df.to_string())
    
    # 再次測試健康檢查(此時快取已建立)
    print("\n" + "="*60)
    print("測試3: 再次健康檢查(快取已建立)")
    print("-" * 60)
    health_status = health_check()
    print(f"健康狀態: {health_status}")
    
    print("\n" + "="*60)
    print("測試完成!")
    print("="*60 + "\n")
