#!/usr/bin/env python3
"""Historical study of the biggest daily gainers: what news moved them, when the move started,
and whether the bot alerted. Runs on a GitHub runner (backtest-recent workflow):

    python tools/gainers.py history 30      # last 30 trading days

Data: Nasdaq screener (universe), Nasdaq daily history (gainers), Nasdaq press releases (titles
by date), SEC submissions (8-K / 6-K acceptance times), Yahoo 1-minute bars (move start, last 29
days), and the bot's own alert log (state.json). Output: backtest-out/gainers/report.md + rows.jsonl
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import bot  # noqa: E402
from backtest import Http  # noqa: E402

OUT = Path("backtest-out/gainers")
HIST_URL = "https://api.nasdaq.com/api/quote/{symbol}/historical?assetclass=stocks&fromdate={start}&limit=80"
SEC_ITEM_CATS = {"1.01": "חוזה / הזמנה", "2.01": "מיזוג / רכישה", "2.02": "דוחות / תחזית", "8.01": "אחר",
                 "7.01": "אחר", "3.02": "הנפקה / איחוד מניות", "5.03": "הנפקה / איחוד מניות"}


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def num(raw: Any) -> float | None:
    return bot._pct_number(raw)


def daily_rows(data: dict[str, Any]) -> list[tuple[dt.date, float, float, float]]:
    """(date, close, high, volume) oldest first from a Nasdaq historical response."""
    rows = (((data or {}).get("data") or {}).get("tradesTable") or {}).get("rows") or []
    out = []
    for r in rows:
        try:
            d = dt.datetime.strptime(r["date"], "%m/%d/%Y").date()
        except (KeyError, ValueError):
            continue
        close, high, vol = num(r.get("close")), num(r.get("high")), num(r.get("volume"))
        if close and high and vol is not None:
            out.append((d, close, high, vol))
    return sorted(out)


def find_gainer_days(sym: str, rows: list[tuple[dt.date, float, float, float]], since: dt.date) -> list[dict[str, Any]]:
    out = []
    for (d0, c0, _, _), (d1, c1, h1, v1) in zip(rows, rows[1:]):
        if d1 < since or c0 <= 0:
            continue
        pct = (c1 / c0 - 1) * 100
        if pct >= bot.GAINERS_MIN_PCT and c1 >= bot.GAINERS_MIN_PRICE and v1 >= bot.GAINERS_MIN_VOLUME:
            out.append({"ticker": sym, "date": d1.isoformat(), "pct": pct, "high_pct": (h1 / c0 - 1) * 100,
                        "prev_close": c0, "close": c1, "volume": v1})
    return out


async def study(days: int) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    tz = bot.eastern_tz()
    today = bot.us_eastern_now().date()
    since = today - dt.timedelta(days=int(days * 1.45) + 2)
    state = json.loads(Path("state.json").read_text(encoding="utf-8")) if Path("state.json").exists() else {}
    alerts = state.get("alert_log", [])
    async with httpx.AsyncClient(follow_redirects=True, limits=httpx.Limits(max_connections=10)) as client:
        nasdaq = Http(client, "", rate=3.0)
        yahoo = Http(client, "", rate=1.0)
        sec = Http(client, bot.Config.from_env().sec_user_agent or "research contact@example.com", rate=8.0)
        tickers = bot.TickerMap()
        tickers.load((await sec.get(bot.TICKERS_EXCHANGE_URL)).json())
        scr = await nasdaq.get(bot.NASDAQ_SCREENER_URL, headers=bot.NASDAQ_HEADERS, sec=False)
        rows = ((scr.json().get("data") or {}).get("rows")) or []
        meta = {bot.normalize_ticker(r["symbol"]): r for r in rows}
        universe = sorted(
            s for s, r in meta.items()
            if s in tickers.by_ticker and (num(r.get("lastsale")) or 0) >= 0.2
            and (num(r.get("marketCap")) or 0) < 50e9 and not (len(s) >= 5 and s[-1] in "WUR"))
        log(f"universe: {len(universe)} listed stocks; history since {since}")

        gainers: list[dict[str, Any]] = []

        async def history(sym: str) -> None:
            try:
                r = await nasdaq.get(HIST_URL.format(symbol=sym, start=since.isoformat()),
                                     headers=bot.NASDAQ_HEADERS, sec=False)
                if r.status_code == 200:
                    gainers.extend(find_gainer_days(sym, daily_rows(r.json()), since + dt.timedelta(days=3)))
            except Exception as exc:  # noqa: BLE001
                log(f"{sym}: {bot.describe_error(exc)}")

        for i in range(0, len(universe), 30):
            await asyncio.gather(*(history(s) for s in universe[i:i + 30]))
            if i % 600 == 0:
                log(f"history {i}/{len(universe)}: {len(gainers)} gainer-days so far")
        gainers.sort(key=lambda g: (g["date"], -g["pct"]))
        log(f"{len(gainers)} gainer-days (20%+)")

        by_sym: dict[str, list[dict[str, Any]]] = {}
        for g in gainers:
            by_sym.setdefault(g["ticker"], []).append(g)

        async def attribute(sym: str, days_: list[dict[str, Any]]) -> None:
            m = meta.get(sym, {})
            press_rows: list[dict[str, Any]] = []
            try:
                pr = await nasdaq.get(bot.NASDAQ_PRESS_URL.format(symbol=sym).replace("limit=8", "limit=30"),
                                      headers=bot.NASDAQ_HEADERS, sec=False)
                press_rows = ((pr.json().get("data") or {}).get("rows")) or [] if pr.status_code == 200 else []
            except Exception:  # noqa: BLE001
                pass
            filings: list[tuple[str, str, str]] = []
            found = tickers.lookup(sym)
            if found:
                try:
                    sub = (await sec.get(bot.SEC_SUBMISSIONS_URL.format(cik=found[0]))).json()
                    rec = sub.get("filings", {}).get("recent", {})
                    filings = list(zip(rec.get("form", []), rec.get("acceptanceDateTime", []), rec.get("items", [])))
                except Exception:  # noqa: BLE001
                    pass
            for g in days_:
                day = dt.date.fromisoformat(g["date"])
                prev_day = day - dt.timedelta(days=3 if day.weekday() == 0 else 1)
                titles = [r.get("title", "") for r in press_rows
                          if str(r.get("created", "")).strip() in (f"{day:%b} {day.day}, {day.year}",
                                                                    f"{prev_day:%b} {prev_day.day}, {prev_day.year}")]
                sec_today = [(f, a, it) for f, a, it in filings
                             if f in ("8-K", "6-K") and a[:10] in (day.isoformat(), prev_day.isoformat())]
                title = titles[0] if titles else ""
                cat = bot.classify_catalyst(title)
                if not title and sec_today:
                    items = [x for x in (sec_today[0][2] or "").split(",") if x and x != "9.01"]
                    cat = SEC_ITEM_CATS.get(items[0], "אחר") if items else "אחר (דיווח SEC)"
                g.update({"title": title, "cat": cat if (title or sec_today) else bot.NO_NEWS,
                          "sec": [f"{f} {a[11:16]} {it}" for f, a, it in sec_today][:3],
                          "mcap": num(m.get("marketCap")) or 0.0, "sector": m.get("sector", ""),
                          "country": m.get("country", "")})
                # move start from 1-minute bars (Yahoo keeps them ~29 days)
                start_ts = dt.datetime.combine(day, dt.time(4, 0), tzinfo=tz).timestamp()
                if time.time() - start_ts < 28 * 86400:
                    url = bot.YAHOO_CHART_URL.format(symbol=sym, p1=int(start_ts), p2=int(start_ts + 16 * 3600), interval=1)
                    try:
                        r = await yahoo.get(url, headers={"User-Agent": "Mozilla/5.0"}, sec=False)
                        bars = bot.parse_yahoo_chart(r.json()) if r.status_code == 200 else []
                    except Exception:  # noqa: BLE001
                        bars = []
                    prof = bot.move_profile([(b[0], b[2]) for b in bars], g["prev_close"])
                    g.update({"start": prof.get("start"), "peak_pct": prof.get("peak_pct")})
                    if sec_today and prof.get("start"):
                        acc = dt.datetime.fromisoformat(sec_today[0][1].replace("Z", "+00:00")).timestamp()
                        g["sec_lead_min"] = round((prof["start"] - acc) / 60)
                lo = dt.datetime.combine(prev_day, dt.time(16, 0), tzinfo=tz).timestamp()
                hi = dt.datetime.combine(day, dt.time(20, 0), tzinfo=tz).timestamp()
                hits = [a["t"] for a in alerts if a.get("ticker") == sym and lo <= a["t"] <= hi]
                g["bot_alert"] = min(hits) if hits else None
                if hits and g.get("start"):
                    g["bot_lead_min"] = round((g["start"] - min(hits)) / 60)

        syms = list(by_sym)
        for i in range(0, len(syms), 6):
            await asyncio.gather(*(attribute(s, by_sym[s]) for s in syms[i:i + 6]))
            if i % 60 == 0:
                log(f"attributed {i}/{len(syms)} symbols")

    (OUT / "rows.jsonl").write_text("".join(json.dumps(g, ensure_ascii=False) + "\n" for g in gainers), encoding="utf-8")
    report(gainers, state)


def report(rows: list[dict[str, Any]], state: dict[str, Any]) -> None:
    if not rows:
        print("no gainers found")
        return
    n = len(rows)
    days = sorted({r["date"] for r in rows})
    lines = [f"# מה מקפיץ מניות: {n} זינוקים של 20%+ ב-{len(days)} ימי מסחר ({days[0]} עד {days[-1]})", ""]
    with_news = [r for r in rows if r["cat"] != bot.NO_NEWS]
    lines.append(f"- עם חדשות או דיווח באותו יום: {len(with_news)} ({len(with_news) * 100 // n}%)")
    lines.append(f"- בלי חדשות פומביות: {n - len(with_news)} ({(n - len(with_news)) * 100 // n}%)")
    small = [r for r in rows if 0 < r.get("mcap", 0) < 300e6]
    lines.append(f"- שווי שוק מתחת ל-$300M: {len(small) * 100 // n}%; מחיר מתחת ל-$5: "
                 f"{sum(1 for r in rows if r['close'] < 5) * 100 // n}%")
    lines += ["", "## סוג החדשות (מספר · חציון עלייה בסגירה · חציון שיא)", ""]
    cats: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        cats.setdefault(r["cat"], []).append(r)
    for cat, rs in sorted(cats.items(), key=lambda kv: -len(kv[1])):
        lines.append(f"- {cat}: {len(rs)} · {bot._median([x['pct'] for x in rs]):+.0f}% · "
                     f"שיא {bot._median([x['high_pct'] for x in rs]):+.0f}%")
    timed = [r for r in rows if r.get("start")]
    if timed:
        sess: dict[str, int] = {}
        for r in timed:
            sess[bot.session_of(r["start"])] = sess.get(bot.session_of(r["start"]), 0) + 1
        lines += ["", "## מתי הזינוק מתחיל", ""] + [f"- {k}: {v}" for k, v in sorted(sess.items(), key=lambda kv: -kv[1])]
        leads = [r["sec_lead_min"] for r in timed if r.get("sec_lead_min") is not None]
        if leads:
            lines.append(f"- דיווח SEC לפני תחילת הזינוק: {sum(1 for x in leads if x >= 0)}/{len(leads)} "
                         f"(חציון {bot._median(leads):.0f} דק')")
    first_alert = min((a["t"] for a in state.get("alert_log", [])), default=None)
    if first_alert:
        since = dt.datetime.fromtimestamp(first_alert, bot.eastern_tz()).date().isoformat()
        covered = [r for r in rows if r["date"] >= since]
        news_cov = [r for r in covered if r["cat"] != bot.NO_NEWS]
        hit = [r for r in news_cov if r.get("bot_alert")]
        early = [r for r in hit if r.get("bot_lead_min") is not None and r["bot_lead_min"] >= 0]
        lines += ["", f"## הבוט מול הזינוקים עם חדשות (מאז {since}: {len(news_cov)})", "",
                  f"- התריע: {len(hit)}; מתוכם לפני תחילת הזינוק: {len(early)}",
                  f"- לא התריע: {len(news_cov) - len(hit)}"]
        missed = [r for r in news_cov if not r.get("bot_alert")][:25]
        if missed:
            lines += ["", "| טיקר | יום | עלייה | קטגוריה | כותרת |", "|---|---|---|---|---|"]
            for r in missed:
                lines.append(f"| {r['ticker']} | {r['date']} | {r['pct']:+.0f}% | {r['cat']} | {r['title'][:70]} |")
    lines += ["", "## 40 הזינוקים הגדולים", "", "| טיקר | יום | עלייה | שיא | קטגוריה | כותרת / דיווח |", "|---|---|---|---|---|---|"]
    for r in sorted(rows, key=lambda r: -r["pct"])[:40]:
        lines.append(f"| {r['ticker']} | {r['date']} | {r['pct']:+.0f}% | {r['high_pct']:+.0f}% | {r['cat']} | "
                     f"{(r['title'] or ' '.join(r.get('sec', [])))[:70]} |")
    text = "\n".join(lines) + "\n"
    (OUT / "report.md").write_text(text, encoding="utf-8")
    print(text)


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in ("history", "report"):
        print(__doc__)
        return 2
    if sys.argv[1] == "report":
        rows = [json.loads(x) for x in (OUT / "rows.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
        state = json.loads(Path("state.json").read_text(encoding="utf-8")) if Path("state.json").exists() else {}
        report(rows, state)
        return 0
    asyncio.run(study(int(sys.argv[2]) if len(sys.argv) > 2 else 30))
    return 0


if __name__ == "__main__":
    sys.exit(main())
