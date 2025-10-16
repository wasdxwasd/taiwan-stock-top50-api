# 台股成交值排行 FastAPI API

本專案為部署至 [Render](https://render.com) 的 FastAPI 爬蟲服務，可查詢台灣證券交易所與櫃買中心的成交值排行榜。

## 功能說明

- 端點：`/top50`
- 可查詢參數：
  - `date`：查詢日期（格式：yyyymmdd），若不指定則自動向前找有資料的交易日
  - `market`：指定市場（twse / otc / all），預設 all
  - `top_n`：取前幾名，預設 50

## 一鍵部署到 Render

點擊下方按鈕即可自動部署：

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/wasdxwasd/taiwan-stock-top50-api)

