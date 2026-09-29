#!/usr/bin/env python3
"""Probe the historical sources the backtest needs, from a GitHub runner.

Saves raw responses to backtest-out/probe/ so they can be inspected offline."""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

OUT = Path("backtest-out/probe")
SEC_UA = os.environ.get("SEC_USER_AGENT", "").strip() or "StockNewsRadar research contact@example.com"
DAY = ("2026", "03", "10")  # a Tuesday about half a year back

PROBES = [
    ("prn_hour", "wire", "https://www.prnewswire.com/news-releases/news-releases-list/"
     f"?page=1&pagesize=100&month={DAY[1]}&day={DAY[2]}&year={DAY[0]}&hour=08"),
    ("prn_day", "wire", "https://www.prnewswire.com/news-releases/news-releases-list/"
     f"?page=2&pagesize=100&month={DAY[1]}&day={DAY[2]}&year={DAY[0]}"),
    ("globe_date", "wire", f"https://www.globenewswire.com/search/date/{'-'.join(DAY)}"),
    ("globe_date_p2", "wire", f"https://www.globenewswire.com/search/date/{'-'.join(DAY)}?page=2"),
    ("globe_en_date", "wire", f"https://www.globenewswire.com/en/search/date/{'-'.join(DAY)}"),
    ("bw_date", "wire", "https://www.businesswire.com/newsroom?page=1"),
    ("accessnw", "wire", "https://www.accessnewswire.com/newsroom"),
    ("edgar_daily_idx", "sec", f"https://www.sec.gov/Archives/edgar/daily-index/{DAY[0]}/QTR1/form.{''.join(DAY)}.idx"),
    ("edgar_efts", "sec", "https://efts.sec.gov/LATEST/search-index?q=%22press%20release%22&forms=8-K"
     f"&dateRange=custom&startdt={'-'.join(DAY)}&enddt={'-'.join(DAY)}"),
    ("yahoo_1m", "other", "https://query1.finance.yahoo.com/v8/finance/chart/AAPL?interval=1m&range=1d&includePrePost=true"),
]


MARKERS = {"prn": "/news-releases/", "globe": "/news-release/2026/", "edgar_efts": "_source",
           "edgar_daily": "8-K", "yahoo": "timestamp"}


def show_structure(name: str, body: str) -> None:
    """Print a short window of the page around the first news item (no raw pages are stored:
    third-party pages can embed API keys that GitHub push protection rejects)."""
    marker = next((m for k, m in MARKERS.items() if name.startswith(k)), None)
    if not marker:
        return
    count = body.count(marker)
    i = body.find(marker, body.find(marker) + 1 if name.startswith("prn") else 0)
    window = re.sub(r"\s+", " ", body[max(0, i - 700):i + 900]) if i >= 0 else ""
    print(f"--- {name}: {count} x {marker!r}\n{window}\n---", flush=True)
    for m in re.finditer(r"(\d{1,2}:\d{2}\s?(?:ET|AM|PM)[^<]{0,20})", body[:200000]):
        print(f"    time sample: {m.group(1)}")
        break


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    report = []
    with httpx.Client(timeout=20, follow_redirects=True) as client:
        for name, kind, url in PROBES:
            ua = SEC_UA if kind == "sec" else (bot.WIRE_USER_AGENT if kind == "wire" else "Mozilla/5.0")
            headers = {"User-Agent": ua, **(bot.WIRE_HEADERS if kind == "wire" else {})}
            try:
                r = client.get(url, headers=headers)
                body = r.text
                line = f"{name}: {r.status_code} {len(body)} bytes final={r.url}"
                show_structure(name, body)
            except Exception as exc:  # noqa: BLE001
                line = f"{name}: ERROR {bot.describe_error(exc)}"
            print(line, flush=True)
            report.append(line)
        key, secret = os.environ.get("ALPACA_API_KEY_ID", ""), os.environ.get("ALPACA_API_SECRET_KEY", "")
        if key and secret:
            h = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
            for name, url in [
                ("alpaca_bars", "https://data.alpaca.markets/v2/stocks/bars?symbols=AAPL&timeframe=1Min"
                 "&start=2026-03-10T13:00:00Z&end=2026-03-10T14:00:00Z&feed=sip&limit=100"),
                ("alpaca_news", "https://data.alpaca.markets/v1beta1/news?start=2026-03-10T12:00:00Z"
                 "&end=2026-03-10T16:00:00Z&limit=50&include_content=true"),
            ]:
                r = client.get(url, headers=h)
                print(r.text[:1500], flush=True)
                line = f"{name}: {r.status_code} {len(r.text)} bytes"
                print(line, flush=True)
                report.append(line)
        else:
            report.append("alpaca: no keys")
    (OUT / "report.txt").write_text("\n".join(report) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
