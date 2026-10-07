"""Probe the data sources the gainers research needs (prints short extracts only)."""
from __future__ import annotations

import re
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

BROWSER = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/124.0 Safari/537.36"}
URLS = [
    ("screener", "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=day_gainers&count=25"),
    ("screener2", "https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=day_gainers&count=25&formatted=false"),
    ("rss_headline", "https://feeds.finance.yahoo.com/rss/2.0/headline?s=NVTS&region=US&lang=en-US"),
    ("gainers_page", "https://finance.yahoo.com/markets/stocks/gainers/"),
    ("search_news", "https://query1.finance.yahoo.com/v1/finance/search?q=NVTS&newsCount=20&quotesCount=0"),
    ("nasdaq_movers", "https://api.nasdaq.com/api/marketmovers?assetclass=stocks&exchange=nasdaq"),
]


def main() -> None:
    with httpx.Client(timeout=20, follow_redirects=True, headers=BROWSER) as c:
        for name, url in URLS:
            try:
                r = c.get(url, headers={"Accept": "application/json, text/html, application/rss+xml"})
                body = r.text
                print(f"\n=== {name}: {r.status_code} {len(body)} bytes {r.headers.get('content-type')}")
                if name == "gainers_page":
                    syms = re.findall(r'data-symbol="([A-Z.\-]+)"|/quote/([A-Z.\-]+)/', body)
                    flat = sorted({a or b for a, b in syms})
                    print("symbols:", len(flat), flat[:40])
                    print(re.sub(r"\s+", " ", bot.html_to_text(body))[:1500])
                else:
                    print(re.sub(r"\s+", " ", body[:1800]))
            except Exception as exc:  # noqa: BLE001
                print(f"\n=== {name}: ERROR {bot.describe_error(exc)}")


if __name__ == "__main__":
    main()
