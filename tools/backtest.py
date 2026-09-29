#!/usr/bin/env python3
"""Backtest: what if every past alert had been bought exactly 3 minutes after it went out?

Signals come from SEC EDGAR (the one public archive with exact, second-level publication
times): every 8-K / 6-K with a press release, scored by the bot's own rules. The acceptance
time is the signal time. Minute bars come from Alpaca (free account, SIP data back to 2016)
or, for the last 29 days only, from Yahoo.

    python tools/backtest.py collect 2025-09-29 2026-03-27     # signals -> backtest-out/signals/
    python tools/backtest.py trades [--provider alpaca|yahoo] [--max 1000]
    python tools/backtest.py report
    python tools/backtest.py all 2025-09-29 2026-03-27 [--provider ...] [--max 1000] [--out DIR]

Everything is cached under backtest-out/, so a run can be resumed.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import math
import os
import random
import re
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

ET = ZoneInfo("America/New_York")
OUT = Path("backtest-out/main")
SIGNALS = OUT / "signals"
TRADES = OUT / "trades.jsonl"

POSITION_USD = bot.PERF_POSITION_USD
SLIPPAGE = bot.PERF_COST / 2         # per side, used for the "net" figures
HORIZONS = bot.TRADE_HORIZONS
TP_SL = bot.TRADE_TP_SL
EFTS_URL = ("https://efts.sec.gov/LATEST/search-index?forms={forms}&dateRange=custom"
            "&startdt={day}&enddt={day}&from={offset}")
ARCHIVE = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nd}/{acc}-index.htm"
SUBMISSIONS = bot.SEC_SUBMISSIONS_URL


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


class Http:
    """SEC-polite client: 8 requests/second, retries with backoff."""

    def __init__(self, client: httpx.AsyncClient, sec_ua: str, rate: float = 8.0):
        self.client = client
        self.sec_ua = sec_ua
        self.interval = 1.0 / rate
        self.lock = asyncio.Lock()
        self.last = 0.0

    async def get(self, url: str, headers: dict[str, str] | None = None, sec: bool = True,
                  attempts: int = 4) -> httpx.Response:
        h = {"User-Agent": self.sec_ua} if sec else {}
        h.update(headers or {})
        for attempt in range(1, attempts + 1):
            async with self.lock:  # each Http instance has its own rate limit
                wait = self.last + self.interval - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
                self.last = time.monotonic()
            try:
                r = await self.client.get(url, headers=h, timeout=30)
                if r.status_code in (429, 500, 502, 503, 504) and attempt < attempts:
                    await asyncio.sleep(2 ** attempt * (5 if r.status_code == 429 else 1))
                    continue
                return r
            except httpx.TransportError:
                if attempt == attempts:
                    raise
                await asyncio.sleep(2 ** attempt)
        raise RuntimeError("unreachable")


# ---------------------------------------------------------------------------
# collect: EDGAR filings -> scored signals
# ---------------------------------------------------------------------------

TICKER_IN_NAME = re.compile(r"\(([A-Z0-9][A-Z0-9.\-]*(?:,\s*[A-Z0-9][A-Z0-9.\-]*)*)\)\s*\(CIK")


def trading_days(start: dt.date, end: dt.date) -> list[dt.date]:
    days, d = [], start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += dt.timedelta(days=1)
    return days


def parse_accepted(index_html: str) -> dt.datetime | None:
    m = re.search(r"(?is)>\s*Accepted\s*</div>\s*<div[^>]*>\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", index_html)
    if not m:
        return None
    return dt.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=ET)


async def efts_filings(http: Http, day: dt.date) -> dict[str, dict[str, Any]]:
    """All 8-K / 6-K filings of a day from EDGAR full-text search, grouped by accession."""
    filings: dict[str, dict[str, Any]] = {}
    offset, total = 0, None
    while total is None or offset < min(total, 10_000):
        r = await http.get(EFTS_URL.format(forms="8-K,6-K", day=day.isoformat(), offset=offset))
        r.raise_for_status()
        data = r.json()
        hits = data.get("hits", {})
        total = hits.get("total", {}).get("value", 0)
        batch = hits.get("hits", [])
        if not batch:
            break
        for h in batch:
            src = h.get("_source", {})
            adsh = src.get("adsh")
            if not adsh or src.get("root_forms", [""])[0] not in ("8-K", "6-K") or src.get("form", "").endswith("/A"):
                continue
            f = filings.setdefault(adsh, {
                "adsh": adsh, "form": src.get("form"), "ciks": src.get("ciks", []),
                "names": src.get("display_names", []), "items": src.get("items") or [], "exhibits": [],
            })
            if str(src.get("file_type", "")).upper().startswith("EX-99"):
                f["exhibits"].append(src.get("file_type"))
        offset += len(batch)
    return filings


def point_in_time_ticker(names: list[str]) -> str | None:
    for n in names:
        m = TICKER_IN_NAME.search(n)
        if m:
            return bot.normalize_ticker(m.group(1).split(",")[0])
    return None


async def score_filing(http: Http, f: dict[str, Any], tickers: bot.TickerMap) -> dict[str, Any] | None:
    cik = int(f["ciks"][0])
    acc = f["adsh"]
    index_url = ARCHIVE.format(cik=cik, acc_nd=acc.replace("-", ""), acc=acc)
    r = await http.get(index_url)
    if r.status_code != 200:
        return None
    accepted = parse_accepted(r.text)
    picked = bot.pick_document(r.text, index_url)
    if not accepted or not picked:
        return None
    doc_url, is_exhibit = picked
    d = await http.get(doc_url)
    if d.status_code != 200:
        return None
    text = bot.html_to_text(d.text)
    text = text if is_exhibit else bot.skip_cover_page(text)
    body = bot.strip_boilerplate(text)
    ticker = point_in_time_ticker(f["names"])
    current = tickers.tickers_for_cik(cik)
    company = re.sub(r"\s*\(.*$", "", f["names"][0]) if f["names"] else ""
    neg = bot.negative_hit(body[:bot.NEGATIVE_CHARS])
    rules = bot.rule_score(body[:bot.LEAD_CHARS], company, strong_text=body[:bot.STRONG_CHARS], title="")
    return {
        "adsh": acc, "cik": cik, "form": f["form"], "items": f["items"],
        "ticker": ticker or (current[0] if current else None), "listed_now": bool(current),
        "company": company, "accepted": accepted.isoformat(), "ts": accepted.timestamp(),
        "doc": doc_url, "exhibit": is_exhibit, "score": -5 if neg else rules.score,
        "rejected": neg, "reason": rules.reason_he, "lead": re.sub(r"\s+", " ", body[:300]),
    }


async def collect(http: Http, start: dt.date, end: dt.date, tickers: bot.TickerMap) -> None:
    SIGNALS.mkdir(parents=True, exist_ok=True)
    cfg_items = bot.Config.from_env().candidate_items
    for day in trading_days(start, end):
        path = SIGNALS / f"{day.isoformat()}.jsonl"
        if path.exists() and path.stat().st_size > 0:
            continue
        try:
            filings = await efts_filings(http, day)
        except Exception as exc:  # noqa: BLE001
            log(f"{day}: EFTS failed: {bot.describe_error(exc)}")
            continue
        # Same filter as the bot's EDGAR path; the document (EX-99.x, else the 8-K itself) is
        # picked from the filing index, because search hits do not list every exhibit.
        cands = [f for f in filings.values() if f["ciks"] and (
            f["form"] == "6-K" or set(f["items"]) & cfg_items)]
        results = await asyncio.gather(*(score_filing(http, f, tickers) for f in cands), return_exceptions=True)
        rows = [r for r in results if isinstance(r, dict) and r.get("ticker")]
        errors = sum(1 for r in results if isinstance(r, Exception))
        tmp = path.with_suffix(".tmp")
        tmp.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), encoding="utf-8")
        tmp.replace(path)
        strong = sum(1 for r in rows if r["score"] >= 4 and not r["rejected"])
        exhibits = sum(1 for r in rows if r["exhibit"])
        log(f"{day}: {len(filings)} filings, {len(cands)} candidates, {len(rows)} scored "
            f"({exhibits} press releases), {strong} alerts (score 4+), {errors} errors")
        for r in results:
            if isinstance(r, Exception):
                log(f"  error: {bot.describe_error(r)}")
                break


def load_signals(min_score: int = 4, dedup_hours: float = 6.0) -> list[dict[str, Any]]:
    rows = []
    for p in sorted(SIGNALS.glob("*.jsonl")):
        rows += [json.loads(ln) for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    rows.sort(key=lambda r: r["ts"])
    last: dict[str, float] = {}
    out = []
    for r in rows:
        if r["rejected"] or r["score"] < min_score:
            continue
        if r["ticker"] in last and r["ts"] - last[r["ticker"]] < dedup_hours * 3600:
            continue  # the bot sends one alert per ticker per 6 hours
        last[r["ticker"]] = r["ts"]
        out.append(r)
    return out


# ---------------------------------------------------------------------------
# prices
# ---------------------------------------------------------------------------

Bar = bot.Bar


async def alpaca_bars(http: Http, symbol: str, start: float, end: float) -> list[Bar]:
    key, secret = os.environ["ALPACA_API_KEY_ID"], os.environ["ALPACA_API_SECRET_KEY"]
    h = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    iso = lambda t: dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
    bars: list[Bar] = []
    token = None
    while True:
        url = (f"https://data.alpaca.markets/v2/stocks/bars?symbols={symbol}&timeframe=1Min"
               f"&start={iso(start)}&end={iso(end)}&feed=sip&adjustment=raw&limit=10000")
        if token:
            url += f"&page_token={token}"
        r = await http.get(url, headers=h, sec=False)
        if r.status_code == 429:
            await asyncio.sleep(10)
            continue
        r.raise_for_status()
        data = r.json()
        for b in (data.get("bars") or {}).get(symbol, []):
            ts = dt.datetime.fromisoformat(b["t"].replace("Z", "+00:00")).timestamp()
            bars.append((ts, b["o"], b["h"], b["l"], b["c"], b["v"]))
        token = data.get("next_page_token")
        if not token:
            return bars


def yahoo_interval(start: float, now: float | None = None) -> int:
    """Yahoo keeps 1-minute bars for 30 days and 2-minute bars for 60 days."""
    age = (now or time.time()) - start
    return 1 if age < 29 * 86400 else 2


async def yahoo_bars(http: Http, symbol: str, start: float, end: float) -> list[Bar]:
    bars: list[Bar] = []
    interval = yahoo_interval(start)
    t = start
    while t < end:
        t2 = min(end, t + 7 * 86400)
        url = bot.YAHOO_CHART_URL.format(symbol=symbol, p1=int(t), p2=int(t2), interval=interval)
        r = await http.get(url, headers={"User-Agent": "Mozilla/5.0"}, sec=False)
        if r.status_code == 200:
            bars += bot.parse_yahoo_chart(r.json())
        t = t2
    return sorted(set(bars))


# ---------------------------------------------------------------------------
# trade simulation: shared with the bot's daily performance report
# ---------------------------------------------------------------------------

session_of = bot.session_of
tp_sl = bot.tp_sl
simulate = bot.simulate_trade


async def pump_level(http: Http, cik: int, when: dt.date, lead: str,
                     cache: dict[int, Any]) -> str:
    if cik not in cache:
        r = await http.get(SUBMISSIONS.format(cik=cik))
        cache[cik] = r.json() if r.status_code == 200 else None
    subs = cache[cik]
    if subs:  # only filings made before the signal (point in time)
        rec = subs.get("filings", {}).get("recent", {})
        keep = [i for i, d in enumerate(rec.get("filingDate", [])) if d < when.isoformat()]
        subs = {"filings": {"recent": {k: [v[i] for i in keep] for k, v in rec.items()
                                       if isinstance(v, list) and len(v) == len(rec.get("form", []))}}}
    return bot.assess_pump_risk(lead, subs, when).level or "none"


async def trades(http: Http, prices: Http, provider: str, max_trades: int, seed: int = 7) -> None:
    signals = load_signals()
    now = time.time()
    if provider == "yahoo":  # 1-minute bars back 29 days, 2-minute bars back 59 days
        signals = [s for s in signals if now - s["ts"] < 57 * 86400]
    done = {}
    if TRADES.exists():
        done = {json.loads(ln)["adsh"]: 1 for ln in TRADES.read_text(encoding="utf-8").splitlines() if ln.strip()}
    random.Random(seed).shuffle(signals)
    signals = signals[:max_trades]
    todo = [s for s in signals if s["adsh"] not in done]
    log(f"{len(signals)} signals selected, {len(todo)} still to price ({provider})")
    fetch = alpaca_bars if provider == "alpaca" else yahoo_bars
    sem = asyncio.Semaphore(4)
    subs_cache: dict[int, Any] = {}

    async def one(s: dict[str, Any]) -> dict[str, Any] | None:
        async with sem:
            start = s["ts"] - 2 * 3600
            end = s["ts"] + 4 * 86400
            try:
                bars = await fetch(prices, s["ticker"], start, min(end, now - 20 * 60))
            except Exception as exc:  # noqa: BLE001
                return {**s, "error": bot.describe_error(exc)}
            sim = simulate(bars, s["ts"])
            if sim is None:
                return {**s, "error": "no bars"}
            sim["bar_min"] = 1 if provider == "alpaca" else yahoo_interval(start)
            day = dt.datetime.fromtimestamp(s["ts"], ET).date()
            try:
                sim["pump"] = await pump_level(http, s["cik"], day, s["lead"], subs_cache)
            except Exception:  # noqa: BLE001
                sim["pump"] = "unknown"
            return {**s, **sim}

    with TRADES.open("a", encoding="utf-8") as fh:
        for i in range(0, len(todo), 40):
            for r in await asyncio.gather(*(one(s) for s in todo[i:i + 40])):
                if r:
                    fh.write(json.dumps(r, ensure_ascii=False) + "\n")
            fh.flush()
            log(f"priced {min(i + 40, len(todo))}/{len(todo)}")


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------


@dataclass
class Stat:
    n: int
    win: float
    mean: float
    median: float
    pnl_usd: float
    net_mean: float
    net_pnl_usd: float


def stat(values: list[float]) -> Stat | None:
    v = [x for x in values if x is not None and not math.isnan(x)]
    if not v:
        return None
    net = [x - 2 * SLIPPAGE for x in v]
    return Stat(len(v), sum(1 for x in v if x > 0) / len(v), statistics.fmean(v), statistics.median(v),
                sum(v) * POSITION_USD, statistics.fmean(net), sum(net) * POSITION_USD)


def pct(x: float | None) -> str:
    return "—" if x is None else f"{x * 100:+.2f}%"


def usd(x: float) -> str:
    return f"{'-' if x < 0 else '+'}${abs(x):,.0f}"


def table(title: str, groups: list[tuple[str, list[float]]]) -> list[str]:
    lines = [f"\n### {title}\n", "| קבוצה | עסקאות | הצלחה | ממוצע | חציון | רווח ברוטו ($2,000 לעסקה) | ממוצע נטו | רווח נטו |",
             "|---|---|---|---|---|---|---|---|"]
    for name, values in groups:
        s = stat(values)
        if s:
            lines.append(f"| {name} | {s.n} | {s.win * 100:.0f}% | {pct(s.mean)} | {pct(s.median)} | "
                         f"{usd(s.pnl_usd)} | {pct(s.net_mean)} | {usd(s.net_pnl_usd)} |")
    return lines


def report() -> dict[str, Any]:
    rows = [json.loads(ln) for ln in TRADES.read_text(encoding="utf-8").splitlines() if ln.strip()]
    ok = [r for r in rows if "error" not in r]
    live = [r for r in ok if r["tradable"]]
    lines = ["# בקטסט: קנייה 3 דקות אחרי ההתראה", ""]
    first = min((r["accepted"] for r in rows), default="")[:10]
    last = max((r["accepted"] for r in rows), default="")[:10]
    lines += [f"- תקופה: {first} עד {last}",
              f"- איתותים (ציון 4+, בלי פסולים, אחד לטיקר ל-6 שעות): {len(rows)}",
              f"- עם נתוני מחיר: {len(ok)}; ניתן לקנות תוך 15 דקות מהיעד: {len(live)}",
              f"- כניסה: פתיחת נר הדקה הראשון שמתחיל 3:00 דקות או יותר אחרי האיתות "
              f"(עיכוב בפועל: חציון {statistics.median([r['entry_delay_s'] for r in live]) if live else 0:.0f} שניות)",
              f"- נטו = אחרי {SLIPPAGE * 100:.1f}% החלקה/מרווח בכל צד", ""]
    later = [r for r in ok if not r["tradable"]]
    horizons = [(f"{h} דקות", [r[f"r_{h}m"] for r in live]) for h in HORIZONS]
    horizons += [("סגירת היום", [r["r_close"] for r in live]),
                 ("סגירה ביום המסחר הבא", [r["r_next_close"] for r in live])]
    lines += table("החזקה קבועה אחרי הכניסה (כל העסקאות הזמינות)", horizons)
    lines += table("יעד רווח / סטופ באותו יום (יציאה בסוף היום אם לא הושג)",
                   [(f"+{int(tp * 100)}% / -{int(sl * 100)}%", [r[f"tp{int(tp * 100)}_sl{int(sl * 100)}"] for r in live])
                    for tp, sl in TP_SL])
    lines += table("איתותים כשהשוק סגור לגמרי (20:00–04:00 / סוף שבוע): קנייה בפתיחת הסשן הבא",
                   [(f"{h} דקות", [r[f"r_{h}m"] for r in later]) for h in (5, 30)]
                   + [("סגירת היום", [r["r_close"] for r in later])])
    key = "r_30m"
    by = lambda f: sorted({f(r) for r in live})  # noqa: E731
    lines += table("החזקה 30 דקות, לפי ציון", [(f"ציון {k}", [r[key] for r in live if r["score"] == k])
                                                for k in by(lambda r: r["score"])])
    names = {"pre": "טרום מסחר", "regular": "מסחר רגיל", "after": "אחרי המסחר", "closed": "שוק סגור (כניסה בפתיחה)"}
    lines += table("החזקה 30 דקות, לפי מועד האיתות", [(names[k], [r[key] for r in live if r["session"] == k])
                                                       for k in by(lambda r: r["session"])])

    def bucket(px: float) -> str:
        return "מתחת ל-$1" if px < 1 else "$1–5" if px < 5 else "$5–20" if px < 20 else "מעל $20"
    lines += table("החזקה 30 דקות, לפי מחיר המניה", [(b, [r[key] for r in live if bucket(r["entry"]) == b])
                                                      for b in ("מתחת ל-$1", "$1–5", "$5–20", "מעל $20")])
    pm = lambda r: ("לא ידוע" if r["pre_move"] is None else "עלתה 10%+ לפני הכניסה" if r["pre_move"] >= 0.10  # noqa: E731
                    else "עלתה 3–10% לפני הכניסה" if r["pre_move"] >= 0.03 else "פחות מ-3% לפני הכניסה")
    lines += table("החזקה 30 דקות, לפי כמה המניה כבר זזה עד הכניסה",
                   [(k, [r[key] for r in live if pm(r) == k]) for k in by(pm)])
    pump_he = {"high": "🔴 סיכון גבוה", "medium": "🟠 סיכון בינוני", "none": "ללא אזהרה", "unknown": "לא ידוע"}
    pumps = [k for k in ("high", "medium", "none", "unknown") if any(r.get("pump", "unknown") == k for r in live)]
    lines += table("החזקה 30 דקות, לפי אזהרת פמפום (מחושבת לפי הדיווחים שהיו עד אותו יום)",
                   [(pump_he[k], [r[key] for r in live if r.get("pump", "unknown") == k]) for k in pumps])
    lines += table("סגירת היום, לפי אזהרת פמפום",
                   [(pump_he[k], [r["r_close"] for r in live if r.get("pump", "unknown") == k]) for k in pumps])
    lines += table("החזקה 30 דקות, לפי רזולוציית הנתונים (נרות 2 דקות: כניסה בין 3:00 ל-4:59)",
                   [(f"נרות של {k} דק'", [r[key] for r in live if r.get("bar_min", 1) == k])
                    for k in by(lambda r: r.get("bar_min", 1))])
    lines += table("החזקה 30 דקות, 8-K מול 6-K", [(k, [r[key] for r in live if r["form"] == k])
                                                  for k in by(lambda r: r["form"])])
    up = [r["max_up_60m"] for r in live]
    if up:
        lines += ["", f"- חציון העלייה המקסימלית בשעה שאחרי הכניסה: {pct(statistics.median(up))}; "
                  f"חציון הירידה המקסימלית: {pct(statistics.median([r['max_down_60m'] for r in live]))}"]
    best = sorted(live, key=lambda r: -r["r_close"])[:10]
    worst = sorted(live, key=lambda r: r["r_close"])[:10]
    for title, group in (("10 הטובות (סגירת היום)", best), ("10 הגרועות (סגירת היום)", worst)):
        lines += [f"\n### {title}\n", "| טיקר | זמן איתות (ניו יורק) | ציון | כניסה | 30 דק' | סגירה | פמפום |",
                  "|---|---|---|---|---|---|---|"]
        for r in group:
            lines.append(f"| {r['ticker']} | {r['accepted'][:16].replace('T', ' ')} | {r['score']} | "
                         f"${r['entry']:.2f} | {pct(r['r_30m'])} | {pct(r['r_close'])} | "
                         f"{pump_he.get(r.get('pump', 'unknown'), '')} |")
    text = "\n".join(lines) + "\n"
    (OUT / "report.md").write_text(text, encoding="utf-8")
    print(text)
    return {"rows": len(rows), "priced": len(ok), "tradable": len(live)}


# ---------------------------------------------------------------------------


async def amain(args: argparse.Namespace) -> int:
    ua = os.environ.get("SEC_USER_AGENT", "").strip()
    if not ua and args.mode in ("collect", "all"):
        print("SEC_USER_AGENT is required")
        return 2
    limits = httpx.Limits(max_connections=20)
    async with httpx.AsyncClient(limits=limits, follow_redirects=True) as client:
        http = Http(client, ua or "research contact@example.com")
        if args.mode in ("collect", "all"):
            tickers = bot.TickerMap()
            r = await http.get(bot.TICKERS_EXCHANGE_URL)
            tickers.load(r.json())
            await collect(http, dt.date.fromisoformat(args.start), dt.date.fromisoformat(args.end), tickers)
        if args.mode in ("trades", "all"):
            provider = args.provider or ("alpaca" if os.environ.get("ALPACA_API_KEY_ID") else "yahoo")
            await trades(http, Http(client, "", rate=3.0), provider, args.max)
        if args.mode in ("report", "trades", "all"):
            report()
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["collect", "trades", "report", "all"])
    p.add_argument("start", nargs="?")
    p.add_argument("end", nargs="?")
    p.add_argument("--provider", choices=["alpaca", "yahoo"])
    p.add_argument("--max", type=int, default=1000)
    p.add_argument("--out", default="backtest-out/main", help="results directory")
    args = p.parse_args()
    global OUT, SIGNALS, TRADES
    OUT = Path(args.out)
    SIGNALS, TRADES = OUT / "signals", OUT / "trades.jsonl"
    OUT.mkdir(parents=True, exist_ok=True)
    return asyncio.run(amain(args))


if __name__ == "__main__":
    sys.exit(main())
