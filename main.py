from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from spider import get_taiwan_stock_data
from datetime import datetime, timedelta

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def find_latest_available_date(market='all', top_n=50, max_lookback=10):
    today = datetime.today()
    for i in range(max_lookback):
        date = today - timedelta(days=i)
        date_str = date.strftime("%Y%m%d")
        try:
            df = get_taiwan_stock_data(date_str, top_n=top_n, market=market)
            if not df.empty and df.shape[1] >= 5:
                return df, date_str
        except:
            continue
    raise ValueError("找不到可用的交易資料（請確認是否為交易日）")

@app.get("/top50")
def top50(
    date: str = Query(None, description="查詢日期 (格式: yyyymmdd)"),
    market: str = Query("all", enum=["twse", "otc", "all"]),
    top_n: int = Query(50, ge=1, le=100)
):
    try:
        if date:
            df = get_taiwan_stock_data(date, top_n=top_n, market=market)
            return {"date_used": date, "data": df.to_dict(orient="records")}
        else:
            df, used_date = find_latest_available_date(market=market, top_n=top_n)
            return {"date_used": used_date, "data": df.to_dict(orient="records")}
    except Exception as e:
        return {"error": str(e)}
