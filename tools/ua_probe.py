"""Which Yahoo endpoints answer an honestly identified client, and what the gainers pages contain."""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

UA = {"User-Agent": "StockNewsRadar/1.0 (+https://github.com/Gilhzn/Telebot)", "Accept": "application/json, text/html"}
URLS = [
    "https://query1.finance.yahoo.com/v8/finance/spark?symbols=AAPL,SXTC,TSLA&range=5d&interval=1d",
    "https://query1.finance.yahoo.com/v8/finance/spark?symbols=AAPL,SXTC&range=3mo&interval=1d",
    "https://finance.yahoo.com/markets/stocks/gainers/?count=100",
    "https://finance.yahoo.com/markets/stocks/small-cap-gainers/?count=100",
    "https://finance.yahoo.com/research-hub/screener/small_cap_gainers/?count=100",
    "https://finance.yahoo.com/markets/stocks/trending/",
]
for url in URLS:
    t = time.time()
    try:
        r = httpx.get(url, headers=UA, timeout=25, follow_redirects=True)
        body = r.text
        print(f"\n=== {r.status_code} {len(body)}B {time.time() - t:.1f}s {url} -> {r.url}")
        if "spark" in url:
            print(re.sub(r"\s+", " ", body[:700]))
        else:
            rows = re.findall(r'data-testid="data-table-v2-row"(.*?)</tr>', body, re.S)
            print("table rows:", len(rows))
            for row in rows[:6]:
                cells = [c for c in (re.sub(r"\s+", " ", bot.html_to_text(x)).strip()
                                     for x in re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)) if c]
                print("   ", cells[:9])
            if not rows:
                i = body.find("/quote/")
                print(re.sub(r"\s+", " ", body[i - 300:i + 900]) if i > 0 else "no /quote/ links")
    except Exception as exc:  # noqa: BLE001
        print(f"\n=== ERR {bot.describe_error(exc)} {url}")
