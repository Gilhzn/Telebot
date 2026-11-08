#!/usr/bin/env python3
"""Benzinga newsroom study: do the rules predict a rise when they score Benzinga's own write-ups?

The yearly catalyst study measured company releases (SEC filings, PR Newswire, GlobeNewswire, Business
Wire) and the earnings study measured Benzinga's results headlines. The bot also scores every other
Benzinga story (headline + summary) the same way as a release, and in the first week of pushing only
predictions those stories were most of the alerts. This collects a year of them through Alpaca's news
API, keeps the ones the bot would have handled (alpaca_news_item + names_company), scores them like the
bot (title + summary, and the title alone for comparison) and measures the reaction the same way as the
other studies (catalysts.outcome).

    python tools/benzinga_study.py collect 2025-10-01 2026-10-31   # on a runner, needs Alpaca keys
    python tools/benzinga_study.py report                           # backtest-out/benzinga/items.jsonl.gz

Output: backtest-out/benzinga/items.jsonl.gz (every scored story with its outcome) and report.md
"""
from __future__ import annotations

import asyncio
import datetime as dt
import gzip
import json
import os
import statistics
import sys
import time
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import bot  # noqa: E402
from catalysts import outcome  # noqa: E402

OUT = Path("backtest-out/benzinga")
DATA = "https://data.alpaca.markets"


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def score_item(title: str, summary: str, company: str) -> dict[str, Any]:
    """The bot's rules score for a Benzinga story (evaluate(): negative rules, rule_score, the
    results-release cap), with the summary and with the headline alone."""
    body = bot.strip_boilerplate(summary)
    neg = bot.negative_hit(f"{title}\n{body[:bot.NEGATIVE_CHARS]}")
    full = bot.rule_score(f"{title}\n{body[:bot.LEAD_CHARS]}", company,
                          strong_text=f"{title}\n{body[:bot.STRONG_CHARS]}", title=title)
    head = bot.rule_score(title, company, strong_text=title, title=title)
    score = full.score
    if bot.EARNINGS_RE.search(f"{title}\n{body[:bot.LEAD_CHARS]}") and "העלאת תחזית" not in full.labels and score > 3:
        score = 3
    return {"neg": neg, "score": -5 if neg else score, "labels": full.labels, "title_score": head.score,
            "title_labels": head.labels}


async def collect(start: str, end: str) -> None:
    key, secret = os.environ.get("ALPACA_API_KEY_ID", ""), os.environ.get("ALPACA_API_SECRET_KEY", "")
    if not key:
        raise SystemExit("ALPACA_API_KEY_ID is not set")
    auth = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    OUT.mkdir(parents=True, exist_ok=True)
    sec_ua = bot.Config.from_env().sec_user_agent or "StockNewsRadar research contact@example.com"
    async with httpx.AsyncClient(timeout=30) as client:
        async def get(url: str, params: dict[str, Any], headers: dict[str, str]) -> httpx.Response:
            for attempt in range(6):
                try:
                    r = await client.get(url, params=params, headers=headers)
                except httpx.HTTPError:
                    await asyncio.sleep(5 * (attempt + 1))
                    continue
                if r.status_code == 429 or r.status_code >= 500:
                    await asyncio.sleep(5 * (attempt + 1))
                    continue
                return r
            raise RuntimeError(f"{url} kept failing")

        tickers = bot.TickerMap()
        tickers.load((await client.get(bot.TICKERS_EXCHANGE_URL, headers={"User-Agent": sec_ua})).json())
        log(f"ticker map: {len(tickers.by_ticker)}")

        # 1. every Benzinga story the bot would score (not results headlines: the earnings study has those)
        raw = OUT / f"stories-{start}-{end}.jsonl.gz"
        items: list[dict[str, Any]] = []
        token, pages, seen = None, 0, 0
        if raw.exists():    # a rerun after a failure later on reuses the collected stories
            with gzip.open(raw, "rt", encoding="utf-8") as f:
                items = [json.loads(x) for x in f if x.strip()]
            log(f"{len(items)} stories from {raw}")
        crawl = not items
        while crawl:
            params = {"start": f"{start}T00:00:00Z", "end": f"{end}T23:59:59Z", "limit": 50, "sort": "asc",
                      "include_content": "false"}
            if token:
                params["page_token"] = token
            r = await get(f"{DATA}/v1beta1/news", params, auth)
            r.raise_for_status()
            data = r.json()
            for n in data.get("news", []):
                seen += 1
                it = bot.alpaca_news_item({**n, "T": "n"})
                if it is None or bot.EPS_HEAD_RE.search(it.title) or bot.SALES_HEAD_RE.search(it.title) \
                        or bot.GUIDE_RANGE_RE.search(it.title):
                    continue
                listed = [t for t in it.tickers if tickers.lookup(t) and tickers.names_company(t, it.title)]
                if not listed:
                    continue
                company = tickers.lookup(listed[0])[1]
                sc = score_item(it.title, it.summary[:500], company)
                if sc["score"] >= 2 or sc["title_score"] >= 2:
                    items.append({"symbol": listed[0], "company": company, "ts": it.published_ts, "title": it.title,
                                  "summary": it.summary[:500], **sc})
            pages += 1
            token = data.get("next_page_token")
            if pages % 500 == 0:
                log(f"news pages {pages}, items {seen}, kept {len(items)}, at "
                    f"{data['news'][-1]['created_at'] if data.get('news') else '?'}")
            if not token:
                break
            await asyncio.sleep(0.31)
        if crawl:
            log(f"{len(items)} scored stories (2+) out of {seen} news items")
            with gzip.open(raw, "wt", encoding="utf-8") as f:
                f.writelines(json.dumps(i) + "\n" for i in items)

        # 2. daily bars (split-adjusted) and 3. share counts, as in the earnings study
        symbols = sorted({i["symbol"] for i in items})
        bars: dict[str, list[tuple]] = {}
        d0 = (dt.date.fromisoformat(start) - dt.timedelta(days=15)).isoformat()
        # the free plan serves SIP bars up to 15 minutes ago: never ask for a later end
        d1 = min(dt.date.fromisoformat(end) + dt.timedelta(days=5), dt.date.today() - dt.timedelta(days=1)).isoformat()
        for i in range(0, len(symbols), 100):
            alp = {s.replace("-", "."): s for s in symbols[i:i + 100]}
            token = None
            while True:
                params = {"symbols": ",".join(alp), "timeframe": "1Day", "start": d0, "end": d1,
                          "adjustment": "split", "feed": "sip", "limit": 10000}
                if token:
                    params["page_token"] = token
                r = await get(f"{DATA}/v2/stocks/bars", params, auth)
                if r.status_code != 200:
                    log(f"bars {r.status_code}: {r.text[:150]}")
                    break
                data = r.json()
                for sym, rows in (data.get("bars") or {}).items():
                    for b in rows:
                        d = dt.datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(bot.eastern_tz()).date()
                        bars.setdefault(alp.get(sym, sym), []).append((d, b["o"], b["h"], b["l"], b["c"], b["v"]))
                token = data.get("next_page_token")
                if not token:
                    break
                await asyncio.sleep(0.31)
            await asyncio.sleep(0.31)
        log(f"bars for {len(bars)} companies")
        shares: dict[int, list[tuple[str, float]]] = {}
        for year, q in ((2025, 2), (2025, 3), (2025, 4), (2026, 1), (2026, 2), (2026, 3)):
            r = await client.get(bot.SEC_FRAMES_URL.format(period=f"CY{year}Q{q}I"), headers={"User-Agent": sec_ua})
            if r.status_code == 200:
                for x in r.json().get("data", []):
                    shares.setdefault(int(x["cik"]), []).append((x["end"], float(x["val"])))
            await asyncio.sleep(0.2)

    rows = []
    for it in items:
        if not it["ts"]:
            continue
        o = outcome(sorted(bars.get(it["symbol"], [])), it["ts"])
        if not o:
            continue
        cik = tickers.lookup(it["symbol"])[0]
        day = dt.datetime.fromtimestamp(it["ts"], bot.eastern_tz()).date().isoformat()
        known = sorted(shares.get(cik, []))
        sh = [v for end_, v in known if end_ <= day] or [v for _, v in known[:1]]
        rows.append({**it, "mcap": o["ref"] * sh[-1] if sh else None, **o})
    with gzip.open(OUT / "items.jsonl.gz", "wt", encoding="utf-8") as f:
        f.writelines(json.dumps(r) + "\n" for r in rows)
    log(f"{len(rows)} stories with prices")
    report(rows)


def up(r: dict[str, Any]) -> bool:
    return r["high2"] >= 0.10 and r["close_r"] >= 0.05


def down(r: dict[str, Any]) -> bool:
    return r["close_r"] <= -0.05


def report(rows: list[dict[str, Any]]) -> None:
    big = [r for r in rows if (r.get("mcap") or 0) >= 1e9]
    # one story per company per reaction: the first one
    big.sort(key=lambda r: r["ts"])
    seen, first = set(), []
    for r in big:
        k = (r["symbol"], r["ref"])
        if k not in seen:
            seen.add(k)
            first.append(r)
    lines = [f"# Benzinga stories scored by the rules, $1B+ companies ({len(first)} company-reactions)", "",
             "up = +10% high within two days and +5% at the reaction day's close; down = -5% close", "",
             "| group | n | up | down | median close |", "|---|---|---|---|---|"]

    def row(name: str, rs: list[dict[str, Any]], min_n: int = 10) -> None:
        if len(rs) >= min_n:
            lines.append(f"| {name} | {len(rs)} | {sum(map(up, rs)) / len(rs) * 100:.1f}% | "
                         f"{sum(map(down, rs)) / len(rs) * 100:.1f}% | "
                         f"{statistics.median(r['close_r'] for r in rs) * 100:+.1f}% |")

    for s in (5, 4, 3, 2):
        row(f"score {s} (title + summary)", [r for r in first if r["score"] == s])
    for s in (5, 4, 3, 2):
        row(f"title alone {s}", [r for r in first if r["title_score"] == s])
    alert = [r for r in first if r["score"] >= 4]
    row("alerted (4+), title alone 4+", [r for r in alert if r["title_score"] >= 4])
    row("alerted (4+), title alone below 4", [r for r in alert if r["title_score"] < 4])
    for lo, hi, nm in ((1e9, 2e9, "1-2B"), (2e9, 10e9, "2-10B"), (10e9, 50e9, "10-50B"), (50e9, 1e16, "50B+")):
        row(f"alerted, {nm}", [r for r in alert if lo <= r["mcap"] < hi])
    for ses in ("pre", "regular", "after", "closed"):
        row(f"alerted, session {ses}", [r for r in alert if r.get("session") == ses])
    labels: dict[str, list[dict[str, Any]]] = {}
    for r in alert:
        for lbl in r["labels"]:
            labels.setdefault(lbl, []).append(r)
    lines += ["", "## alerted stories by rule label", "", "| label | n | up | down | median close |", "|---|---|---|---|---|"]
    for lbl, rs in sorted(labels.items(), key=lambda kv: -len(kv[1])):
        row(lbl, rs, 8)
    lines += ["", "## samples (alerted)", ""]
    for r in alert[:: max(1, len(alert) // 60)][:60]:
        lines.append(f"- {'UP' if up(r) else 'DOWN' if down(r) else '  '} {r['symbol']} s{r['score']}/t{r['title_score']} "
                     f"{r['close_r'] * 100:+.1f}% {r['title'][:100]} — {', '.join(r['labels'][:3])}")
    text = "\n".join(lines) + "\n"
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "report.md").write_text(text, encoding="utf-8")
    print(text)


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "collect" and len(sys.argv) >= 4:
        asyncio.run(collect(sys.argv[2], sys.argv[3]))
        return 0
    if mode == "report":
        with gzip.open(OUT / "items.jsonl.gz", "rt", encoding="utf-8") as f:
            report([json.loads(x) for x in f if x.strip()])
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
