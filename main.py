# -*- coding: utf-8 -*-
from fastapi import FastAPI, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from spider import get_taiwan_stock_data
from datetime import datetime, timedelta
import numpy as np
import os
import logging

# === 基本設定 ===
app = FastAPI(title="Taiwan Stock Top50 API", version="1.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Logging 設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uvicorn.error")

# === 輔助函數 ===
def find_latest_available_date(market='all', top_n=50, max_lookback=10):
    today = datetime.today()
    for i in range(max_lookback):
        date = today - timedelta(days=i)
        date_str = date.strftime("%Y%m%d")
        try:
            df = get_taiwan_stock_data(date_str, top_n=top_n, market=market)
            if not df.empty and df.shape[1] >= 5:
                return df, date_str
        except Exception as e:
            logger.warning(f"找不到資料: {date_str}, 原因: {e}")
            continue
    raise ValueError("找不到可用的交易資料")

# === 主要端點 ===
@app.api_route("/top50", methods=["GET", "HEAD"])
def top50(
    request: Request,
    date: str = Query(None, description="查詢日期 (格式: yyyymmdd)"),
    market: str = Query("all", enum=["twse", "otc", "all"]),
    top_n: int = Query(50, ge=1, le=100)
):
    # HEAD 檢查
    if request.method == "HEAD":
        return Response(status_code=200)

    try:
        if date:
            df = get_taiwan_stock_data(date, top_n=top_n, market=market)
            date_used = date
        else:
            df, date_used = find_latest_available_date(market=market, top_n=top_n)

        df = df.replace({np.nan: None})
        return {"date_used": date_used, "data": df.to_dict(orient="records")}
    except Exception as e:
        logger.error(f"/top50 錯誤: {e}")
        return {"error": str(e), "message": "請確認日期格式或是否為交易日"}

@app.get("/")
def root():
    return {
        "message": "台股成交值排行 API",
        "endpoints": {
            "/top50": "取得成交值排行榜",
            "/health": "健康檢查端點",
            "/docs": "Swagger 文件"
        }
    }

# === 健康檢查端點 ===
@app.api_route("/health", methods=["GET", "HEAD"])
def health_check(request: Request):
    if request.method == "HEAD":
        return Response(status_code=200)
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

# === 啟動點（Render 專用） ===
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
