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
SEC_ITEM_CATS = {"1.01": "חוזה / הזמנה", "2.01": "מיזוג / רכישה", "2.02": "דוחות / תחזית", "8.01": "אחר",
                 "7.01": "אחר", "3.02": "הנפקה / איחוד מניות", "5.03": "הנפקה / איחוד מניות"}


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def gainer_days(data: dict[str, Any], since: dt.date) -> list[dict[str, Any]]:
    """Every (symbol, day) with a 20%+ close-to-close gain in a Yahoo spark response."""
    tz = bot.eastern_tz()
    out = []
    for sym, d in (data or {}).items():
        pairs = [(t, c) for t, c in zip((d or {}).get("timestamp") or [], (d or {}).get("close") or []) if c]
        for (_, c0), (t1, c1) in zip(pairs, pairs[1:]):
            day = dt.datetime.fromtimestamp(t1, tz).date()
            if day < since or c0 <= 0:
                continue
            pct = (c1 / c0 - 1) * 100
            if pct >= bot.GAINERS_MIN_PCT and c1 >= bot.GAINERS_MIN_PRICE:
                out.append({"ticker": bot.normalize_ticker(sym), "date": day.isoformat(), "pct": pct,
                            "prev_close": c0, "close": c1})
    return out


async def study(days: int) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    tz = bot.eastern_tz()
    today = bot.us_eastern_now().date()
    since = today - dt.timedelta(days=int(days * 1.45) + 2)
    state = json.loads(Path("state.json").read_text(encoding="utf-8")) if Path("state.json").exists() else {}
    alerts = state.get("alert_log", [])
    hdr = bot.RESEARCH_HEADERS
    async with httpx.AsyncClient(follow_redirects=True, limits=httpx.Limits(max_connections=10)) as client:
        yahoo = Http(client, "", rate=4.0)
        sec = Http(client, bot.Config.from_env().sec_user_agent or "research contact@example.com", rate=8.0)
        tickers = bot.TickerMap()
        tickers.load((await sec.get(bot.TICKERS_EXCHANGE_URL)).json())
        universe = bot.research_universe(tickers)
        log(f"universe: {len(universe)} listed common shares; gainers since {since}")
        gainers: list[dict[str, Any]] = []
        for i in range(0, len(universe), bot.SPARK_BATCH):
            batch = ",".join(universe[i:i + bot.SPARK_BATCH])
            try:
                r = await yahoo.get(bot.YAHOO_SPARK_URL.format(symbols=batch, range="3mo"), headers=hdr, sec=False)
                if r.status_code == 200:
                    gainers.extend(gainer_days(r.json(), since))
            except Exception as exc:  # noqa: BLE001
                log(f"spark batch {i}: {bot.describe_error(exc)}")
            if i % 1000 == 0:
                log(f"spark {i}/{len(universe)}: {len(gainers)} gainer-days so far")
        gainers.sort(key=lambda g: (g["date"], -g["pct"]))
        log(f"{len(gainers)} gainer-days (20%+)")
        by_sym: dict[str, list[dict[str, Any]]] = {}
        for g in gainers:
            by_sym.setdefault(g["ticker"], []).append(g)

        async def attribute(sym: str, days_: list[dict[str, Any]]) -> None:
            news: list[dict[str, Any]] = []
            try:
                nr = await yahoo.get(bot.YAHOO_NEWS_URL.format(symbol=sym, count=50), headers=hdr, sec=False)
                news = bot.yahoo_news_items(nr.json(), sym) if nr.status_code == 200 else []
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
                day0 = dt.datetime.combine(day, dt.time(4, 0), tzinfo=tz).timestamp()
                bars: list[Any] = []
                if time.time() - day0 < 28 * 86400:
                    try:
                        r = await yahoo.get(bot.YAHOO_CHART_URL.format(symbol=sym, p1=int(day0), p2=int(day0 + 16 * 3600),
                                                                       interval=1), headers=hdr, sec=False)
                        bars = bot.parse_yahoo_chart(r.json()) if r.status_code == 200 else []
                    except Exception:  # noqa: BLE001
                        bars = []
                prof = bot.move_profile([(b[0], b[2]) for b in bars], g["prev_close"])
                start = prof.get("start")
                anchor = start or dt.datetime.combine(day, dt.time(16, 0), tzinfo=tz).timestamp()
                near = [n for n in news if anchor - 24 * 3600 <= n["pub"] <= anchor + 15 * 60]
                first = min(near, key=lambda n: n["pub"]) if near else None
                sec_near = [(f, a, it) for f, a, it in filings if f in ("8-K", "6-K") and a
                            and anchor - 24 * 3600 <= dt.datetime.fromisoformat(a.replace("Z", "+00:00")).timestamp()
                            <= anchor + 15 * 60]
                cat = bot.classify_catalyst(first["title"]) if first else bot.NO_NEWS
                if not first and sec_near:
                    items = [x for x in (sec_near[0][2] or "").split(",") if x and x != "9.01"]
                    cat = SEC_ITEM_CATS.get(items[0], "אחר") if items else "אחר (דיווח SEC)"
                hits = [a["t"] for a in alerts if a.get("ticker") == sym and anchor - 24 * 3600 <= a["t"] <= anchor + 12 * 3600]
                g.update({
                    "volume": sum(b[5] for b in bars) if bars else None, "start": start,
                    "peak_pct": prof.get("peak_pct"), "high_pct": prof.get("peak_pct") or g["pct"],
                    "title": first["title"] if first else "", "news_src": first["src"] if first else "",
                    "news_t": first["pub"] if first else None, "cat": cat,
                    "lead_min": round((start - first["pub"]) / 60) if start and first else None,
                    "sec": [f"{f} {a[11:16]} {it}" for f, a, it in sec_near][:3],
                    "bot_alert": min(hits) if hits else None,
                    "bot_lead_min": round((start - min(hits)) / 60) if hits and start else None,
                    "missing_source": bool(first) and not bot.is_our_source(first["src"]),
                })

        syms = list(by_sym)
        for i in range(0, len(syms), 4):
            await asyncio.gather(*(attribute(s_, by_sym[s_]) for s_ in syms[i:i + 4]))
            if i % 80 == 0:
                log(f"attributed {i}/{len(syms)} symbols")
    rows = [g for g in gainers if g.get("volume") is None or g["volume"] >= bot.GAINERS_MIN_VOLUME]
    (OUT / "rows.jsonl").write_text("".join(json.dumps(g, ensure_ascii=False) + "\n" for g in rows), encoding="utf-8")
    report(rows, state)


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
    lines.append(f"- מחיר מתחת ל-$5: {sum(1 for r in rows if r['close'] < 5) * 100 // n}%")
    srcs: dict[str, list[float]] = {}
    for r in rows:
        if r.get("news_src"):
            srcs.setdefault(r["news_src"], []).append(r["lead_min"] if r.get("lead_min") is not None else float("nan"))
    if srcs:
        lines += ["", "## מאיפה החדשות הגיעו ראשונות (מספר · חציון דקות מהפרסום לתחילת הזינוק)", ""]
        for src, leads in sorted(srcs.items(), key=lambda kv: -len(kv[1]))[:15]:
            real = [x for x in leads if x == x]
            med = f"{bot._median(real):.0f} דק'" if real else "—"
            mark = "" if bot.is_our_source(src) else " ⚠️ הבוט לא קורא"
            lines.append(f"- {src}: {len(leads)} · {med}{mark}")
    lines += ["", "## סוג החדשות (מספר · חציון עלייה בסגירה · חציון שיא)", ""]
    cats: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        cats.setdefault(r["cat"], []).append(r)
    for cat, rs in sorted(cats.items(), key=lambda kv: -len(kv[1])):
        lines.append(f"- {cat}: {len(rs)} · {bot._median([x['pct'] for x in rs]):+.0f}% · "
                     f"שיא {bot._median([x.get('high_pct') or x['pct'] for x in rs]):+.0f}%")
    timed = [r for r in rows if r.get("start")]
    if timed:
        sess: dict[str, int] = {}
        for r in timed:
            sess[bot.session_of(r["start"])] = sess.get(bot.session_of(r["start"]), 0) + 1
        lines += ["", "## מתי הזינוק מתחיל", ""] + [f"- {k}: {v}" for k, v in sorted(sess.items(), key=lambda kv: -kv[1])]
        leads = [r["lead_min"] for r in timed if r.get("lead_min") is not None]
        if leads:
            lines.append(f"- החדשות פורסמו לפני תחילת הזינוק: {sum(1 for x in leads if x >= 0)}/{len(leads)} "
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
        lines.append(f"| {r['ticker']} | {r['date']} | {r['pct']:+.0f}% | {r.get('high_pct') or r['pct']:+.0f}% | {r['cat']} | "
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
