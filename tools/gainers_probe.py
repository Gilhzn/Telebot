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
    ("nq_screener", "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&offset=0&download=true"),
    ("nq_movers", "https://api.nasdaq.com/api/marketmovers?assetclass=stocks&exchange=nasdaq"),
    ("nq_news", "https://api.nasdaq.com/api/news/topic/articlebysymbol?q=SXTC|stocks&offset=0&limit=10&fallback=true"),
    ("nq_press", "https://api.nasdaq.com/api/news/topic/press_release?q=symbol:SXTC|assetclass:stocks&limit=10&offset=0"),
    ("nq_chart", "https://api.nasdaq.com/api/quote/SXTC/chart?assetclass=stocks"),
    ("nq_extended", "https://api.nasdaq.com/api/quote/SXTC/extended-trading?assetclass=stocks&markettype=pre"),
    ("yahoo_chart", "https://query1.finance.yahoo.com/v8/finance/chart/SXTC?interval=1m&range=1d&includePrePost=true"),
    ("yahoo_chart2", "https://query2.finance.yahoo.com/v8/finance/chart/SXTC?interval=2m&range=5d&includePrePost=true"),
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
                    print(re.sub(r"\s+", " ", body[:2500]))
            except Exception as exc:  # noqa: BLE001
                print(f"\n=== {name}: ERROR {bot.describe_error(exc)}")


if __name__ == "__main__":
    main()
