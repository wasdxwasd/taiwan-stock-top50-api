# -*- coding: utf-8 -*-
"""
main.py — FastAPI主程式(極簡啟動版)
"""

from fastapi import FastAPI, HTTPException
from typing import Optional
import uvicorn

# 修正：引用實際的檔案名稱 spider.py
from spider import (
    get_taiwan_stock_data,
    find_latest_available_date,
    health_check
)

# 建立FastAPI應用
app = FastAPI(
    title="Taiwan Stock API",
    description="台股成交值排行榜API - 極簡啟動版",
    version="4.0"
)


# ========== 健康檢查端點 ==========
@app.get("/")
async def root():
    """根路徑健康檢查 - Render會每5秒呼叫一次"""
    return health_check()


@app.get("/health")
async def health():
    """額外的健康檢查端點"""
    return health_check()


# ========== 主要查詢端點 ==========
@app.get("/top50")
async def get_top50(
    date: Optional[str] = None,
    top_n: Optional[int] = 50,
    market: Optional[str] = "all"
):
    """
    查詢成交值排行榜
    
    參數說明:
    - date: 查詢日期(YYYYMMDD),不指定則自動尋找最近交易日
    - top_n: 取前幾名,預設50
    - market: 市場別(all/twse/otc),預設all
    
    使用範例:
    GET /top50?date=20251009&top_n=50&market=all
    GET /top50?top_n=30&market=twse
    GET /top50  (自動尋找最近交易日)
    """
    try:
        if date is None:
            # 自動尋找最近可用交易日
            print(f"\n[/top50] 未指定日期,自動尋找最近交易日...")
            df, found_date = find_latest_available_date(
                market=market,
                top_n=top_n,
                max_lookback=10
            )
            result = {
                "success": True,
                "date": found_date,
                "auto_detected": True,
                "count": len(df),
                "data": df.to_dict(orient="records")
            }
            print(f"[/top50] 自動查詢成功,使用日期 {found_date}\n")
        else:
            # 使用指定日期查詢
            print(f"\n[/top50] 收到查詢請求:")
            print(f"  日期: {date}")
            print(f"  數量: {top_n}")
            print(f"  市場: {market}\n")
            
            df = get_taiwan_stock_data(
                date=date,
                top_n=top_n,
                market=market
            )
            
            result = {
                "success": True,
                "date": date,
                "auto_detected": False,
                "count": len(df),
                "data": df.to_dict(orient="records")
            }
            print(f"[/top50] 查詢成功,回傳 {len(df)} 筆資料\n")
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查詢失敗: {str(e)}")


# ========== 本地測試用 ==========
if __name__ == "__main__":
    print("\n" + "="*60)
    print("本地測試模式啟動")
    print("="*60)
    print("健康檢查: http://127.0.0.1:8000")
    print("API端點:  http://127.0.0.1:8000/top50")
    print("API文件:  http://127.0.0.1:8000/docs")
    print("="*60 + "\n")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
