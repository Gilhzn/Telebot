"""Did Benzinga publish results-number headlines today, and with which symbols? (REST, Alpaca keys)"""
from __future__ import annotations

import datetime as dt
import os
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

auth = {"APCA-API-KEY-ID": os.environ["ALPACA_API_KEY_ID"], "APCA-API-SECRET-KEY": os.environ["ALPACA_API_SECRET_KEY"]}
start = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
token, items = None, []
for _ in range(80):
    p = {"start": start, "limit": 50, "sort": "asc", "include_content": "false"}
    if token:
        p["page_token"] = token
    r = httpx.get("https://data.alpaca.markets/v1beta1/news", params=p, headers=auth, timeout=30).json()
    items += r.get("news", [])
    token = r.get("next_page_token")
    if not token:
        break
    time.sleep(0.35)
eps = [n for n in items if bot.EPS_HEAD_RE.search(n["headline"])]
print(f"{len(items)} news items since {start}, {len(eps)} with EPS numbers")
for n in eps[:25]:
    print(n["id"], n["created_at"], n.get("updated_at"), n.get("author"), n.get("source"), n["symbols"], n["headline"][:90])
