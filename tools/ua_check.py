"""Why does a source refuse the bot? Fetch it with the bot's own client and with other honest
identifications (never a browser's), and print status, server and the start of any refusal page."""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

GLOBE = next(u for u in bot._split(bot.DEFAULT_WIRE_FEEDS) if "orgclass" in u)
URLS = [GLOBE, "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtRWA==",
        "https://www.prnewswire.com/rss/news-releases-list.rss", "https://www.globenewswire.com/RssFeed/industry/9576-Semiconductors/feedTitle/x",
        "https://www.globenewswire.com/newsroom", bot.PRN_LIST_URL,
        "https://query1.finance.yahoo.com/v8/finance/spark?symbols=AAPL,MSFT,NVDA,TSLA,AMD&range=5d&interval=1d"]
AGENTS = {
    "bot wire UA": {"User-Agent": bot.WIRE_USER_AGENT, **bot.WIRE_HEADERS},
    "bot research UA": bot.RESEARCH_HEADERS,
    "compatible-bot UA": {"User-Agent": "Mozilla/5.0 (compatible; StockNewsRadar/1.0; +https://github.com/Gilhzn/Telebot)",
                          "Accept": "application/rss+xml, application/xml, text/xml, */*"},
    "feed-reader UA": {"User-Agent": "StockNewsRadar-RSS/1.0", "Accept": "application/rss+xml"},
    "python default": {},
}
for url in URLS:
    print(f"\n=== {url[:110]}")
    for name, h in AGENTS.items():
        t = time.time()
        try:
            r = httpx.get(url, headers=h, timeout=20, follow_redirects=True)
            body = r.text
            note = "" if r.status_code == 200 else " | " + re.sub(r"\s+", " ", bot.html_to_text(body))[:220]
            print(f"  {name:18} {r.status_code} {len(body):7}B {time.time() - t:4.1f}s server={r.headers.get('server', '')[:20]}"
                  f" cf-ray={'yes' if 'cf-ray' in r.headers else 'no'} akamai={'yes' if 'akamai' in str(r.headers).lower() else 'no'}{note}")
        except Exception as exc:  # noqa: BLE001
            print(f"  {name:18} ERR {bot.describe_error(exc)}")
        time.sleep(1)
# Yahoo spark under load: 40 batches like the bot's scan, count statuses
st: dict[int, int] = {}
syms = sorted(bot.TickerMap().by_ticker) or []
for i in range(40):
    try:
        r = httpx.get(f"https://query1.finance.yahoo.com/v8/finance/spark?symbols=AAPL,MSFT,NVDA,TSLA,AMD,INTC,PCRX,OLB,"
                      f"F,GM,T,VZ,KO,PEP,WMT,COST,HD,LOW,TGT,BA&range=5d&interval=1d&i={i}",
                      headers=bot.RESEARCH_HEADERS, timeout=20)
        st[r.status_code] = st.get(r.status_code, 0) + 1
        if r.status_code == 200 and i == 0:
            print("\nspark symbols returned:", len(r.json()))
    except Exception:  # noqa: BLE001
        st[0] = st.get(0, 0) + 1
    time.sleep(0.25)
print("spark statuses over 40 quick batches:", st)
