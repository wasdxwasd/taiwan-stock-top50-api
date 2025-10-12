# 台股成交值排行 FastAPI API

本專案為部署至 [Render](https://render.com) 的 FastAPI 爬蟲服務，可查詢台灣證券交易所與櫃買中心的成交值排行榜。

## 功能說明

- 端點：`/top50`
- 可查詢參數：
  - `date`：查詢日期（格式：yyyymmdd），若不指定則自動向前找有資料的交易日
  - `market`：指定市場（twse / otc / all），預設 all
  - `top_n`：取前幾名，預設 50

## 使用範例

### 指定日期查詢

GET /top50?date=20251009&top_n=50&market=all


### 自動尋找最近交易日

GET /top50?top_n=30&market=twse


### 完整URL範例

https://your-app-name.onrender.com/top50?date=20251009&top_n=50&market=all


## 回傳格式

{
"success": true,
"date": "20251009",
"auto_detected": false,
"count": 50,
"data": [
{
"證券代號": "2330",
"證券名稱": "台積電",
"成交金額": 61324929553,
"收盤價": 1440.00,
"市場": "上市",
"1日漲跌幅": -0.35,
"5日漲跌幅": 2.14,
...
}
]
}


## 一鍵部署到 Render

點擊下方按鈕即可自動部署：

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/render-examples/taiwan-stock-top50-api)

**注意**: 請將上方連結中的 `YOUR-USERNAME/YOUR-REPO-NAME` 替換為您的實際GitHub repository路徑。

## 本地測試

安裝套件
pip install -r requirements.txt

啟動服務
python main.py

測試端點
curl "http://127.0.0.1:8000/top50?date=20251009&top_n=5"


## 注意事項

1. **首次請求較慢**: 建立交易日快取需約25-30秒
2. **後續請求快速**: 使用快取後只需3-5秒
3. **自動尋找功能**: 不指定date會自動往前找最多10天
4. **免費方案**: Render免費方案15分鐘無請求後會休眠

## API文檔

部署完成後可訪問自動生成的API文檔:
- Swagger UI: `https://your-app-name.onrender.com/docs`
- ReDoc: `https://your-app-name.onrender.com/redoc`
