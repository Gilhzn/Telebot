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
if len(sys.argv) >= 4:     # eps_check.py TREX,XERS 2026-11-03T20:00:00Z 2026-11-06T00:00:00Z: every item, with both times
    for sym in sys.argv[1].split(","):
        r = httpx.get("https://data.alpaca.markets/v1beta1/news", headers=auth, timeout=30, params={
            "symbols": sym, "start": sys.argv[2], "end": sys.argv[3], "limit": 50, "sort": "asc", "include_content": "false"})
        for n in r.json().get("news", []):
            print(sym, n["id"], "created", n["created_at"], "updated", n.get("updated_at"), n.get("author"), n["symbols"][:5],
                  n["headline"][:90])
        time.sleep(0.4)
    sys.exit(0)
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
