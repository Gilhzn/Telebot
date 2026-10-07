"""Does a data host answer an honestly identified client? (prints status and timing only)"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

UAS = {"honest": bot.NASDAQ_HEADERS["User-Agent"], "plain": "StockNewsRadar/1.0 (+https://github.com/Gilhzn/Telebot)"}
URLS = ["https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&offset=0",
        "https://api.nasdaq.com/api/quote/AAPL/chart?assetclass=stocks",
        "https://finance.yahoo.com/markets/stocks/gainers/?count=100",
        "https://query1.finance.yahoo.com/v8/finance/chart/AAPL?interval=1m&range=1d&includePrePost=true"]
for name, ua in UAS.items():
    for url in URLS:
        t = time.time()
        try:
            r = httpx.get(url, headers={"User-Agent": ua, "Accept": "application/json, text/html"}, timeout=25)
            print(f"{name:7} {r.status_code} {len(r.content):>8}B {time.time() - t:5.1f}s {url}")
        except Exception as exc:  # noqa: BLE001
            print(f"{name:7} ERR {bot.describe_error(exc)} {time.time() - t:5.1f}s {url}")
