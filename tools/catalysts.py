#!/usr/bin/env python3
"""One-year catalyst study: which news actually makes a stock jump, and which "good news" does not.

Every 8-K / 6-K press release collected by tools/backtest.py (backtest-out/*/signals) is joined
with the stock's daily prices (Yahoo, split-adjusted), its size (SEC XBRL shares outstanding x
price), sector (SEC SIC code) and dilution history (SEC filings before the event). The outcome is
whether the stock jumped: the highest price on the event day or the next one versus the close
before the event. A logistic model trained on the older 75% of events is tested on the newest 25%.

    python tools/catalysts.py run            # on a GitHub runner (backtest workflow)
    python tools/catalysts.py report         # rebuild the report from backtest-out/catalysts/rows.jsonl

Output: backtest-out/catalysts/report.md, rows.jsonl, model.json
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import math
import random

import sys
import time
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import bot  # noqa: E402
from backtest import Http  # noqa: E402

OUT = Path("backtest-out/catalysts")
JUMP = 0.20                 # a jump: +20% intraday on the event day or the next one
STRONG_CLOSE = 0.10         # and a strong close: +10% at the reaction day's close
MIN_DOLLAR_VOLUME = 300_000
FRAMES_URLS = ("https://data.sec.gov/api/xbrl/frames/dei/EntityCommonStockSharesOutstanding/shares/{period}.json",
               "https://data.sec.gov/api/xbrl/frames/us-gaap/CommonStockSharesOutstanding/shares/{period}.json")
DAILY_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=2y&interval=1d"


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


# ---------------------------------------------------------------------------
# outcome


# ---------------------------------------------------------------------------


def outcome(bars: list[tuple[dt.date, float, float, float, float, float]], ts: float) -> dict[str, Any] | None:
    """How the stock reacted. bars: (date, open, high, low, close, volume) split-adjusted, oldest first.

    The reaction day is the event day for news before 16:00 ET, else the next trading day; the
    reference is the close before it. A reverse split Yahoo has not adjusted yet looks like a gap of
    100%+ with an ordinary day range, and is dropped."""
    et = dt.datetime.fromtimestamp(ts, bot.eastern_tz())
    d = et.date()
    same_day = et.time() < dt.time(16, 0)
    after = [b for b in bars if b[0] > d or (same_day and b[0] == d)]
    if not after:
        return None
    reaction = after[0]
    before = [b for b in bars if b[0] < reaction[0]]
    if not before or before[-1][4] <= 0:
        return None
    ref = before[-1][4]
    window = after[:2]
    open_r = reaction[1] / ref - 1
    if open_r >= 1.0 and reaction[3] > 0 and reaction[2] / reaction[3] < 1.25:
        return None
    week = before[-6:]
    return {
        "ref": ref, "high2": max(b[2] for b in window) / ref - 1, "close_r": reaction[4] / ref - 1,
        "open_r": open_r, "dollar_vol": reaction[4] * reaction[5],
        "pre5": (week[-1][4] / week[0][4] - 1) if len(week) >= 2 and week[0][4] > 0 else 0.0,
        "session": bot.session_of(ts),
    }


# ---------------------------------------------------------------------------
# data collection (runner)


# ---------------------------------------------------------------------------


def load_events() -> list[dict[str, Any]]:
    seen, rows = set(), []
    for p in sorted(Path("backtest-out").glob("*/signals/*.jsonl")):
        for line in p.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            if r["adsh"] in seen or not r.get("ticker"):
                continue
            seen.add(r["adsh"])
            rows.append(r)
    return rows


def parse_daily(data: dict[str, Any]) -> list[tuple[dt.date, float, float, float, float, float]]:
    res = ((data.get("chart") or {}).get("result") or [None])[0] or {}
    q = ((res.get("indicators") or {}).get("quote") or [{}])[0]
    out = []
    tz = bot.eastern_tz()
    for i, t in enumerate(res.get("timestamp") or []):
        vals = [(q.get(k) or [None] * (i + 1))[i] for k in ("open", "high", "low", "close", "volume")]
        if None not in vals[:4]:
            out.append((dt.datetime.fromtimestamp(t, tz).date(), *[float(v or 0) for v in vals]))
    return out


async def collect() -> list[dict[str, Any]]:
    events = load_events()
    log(f"{len(events)} SEC press-release events, {len({e['ticker'] for e in events})} tickers")
    by_ticker: dict[str, list[dict[str, Any]]] = {}
    for e in events:
        by_ticker.setdefault(e["ticker"], []).append(e)
    rows: list[dict[str, Any]] = []
    async with httpx.AsyncClient(follow_redirects=True, limits=httpx.Limits(max_connections=10)) as client:
        yahoo = Http(client, "", rate=4.0)
        sec = Http(client, bot.Config.from_env().sec_user_agent or "research contact@example.com", rate=8.0)
        shares: dict[int, list[tuple[str, float]]] = {}
        for url in FRAMES_URLS:
            for year, qtr in ((y, q) for y in (2025, 2026) for q in (1, 2, 3, 4)):
                try:
                    r = await sec.get(url.format(period=f"CY{year}Q{qtr}I"))
                    if r.status_code == 200:
                        for x in r.json().get("data", []):
                            shares.setdefault(int(x["cik"]), []).append((x["end"], float(x["val"])))
                except Exception as exc:  # noqa: BLE001
                    log(f"frames {url.split('/')[-3]} {year}Q{qtr}: {bot.describe_error(exc)}")
        log(f"shares outstanding for {len(shares)} companies")

        async def one(ticker: str, evs: list[dict[str, Any]]) -> None:
            try:
                r = await yahoo.get(DAILY_URL.format(symbol=ticker), headers=bot.RESEARCH_HEADERS, sec=False)
                bars = parse_daily(r.json()) if r.status_code == 200 else []
            except Exception:  # noqa: BLE001
                bars = []
            if not bars:
                return
            cik = evs[0]["cik"]
            subs: dict[str, Any] = {}
            try:
                subs = (await sec.get(bot.SEC_SUBMISSIONS_URL.format(cik=cik))).json()
            except Exception:  # noqa: BLE001
                pass
            try:
                sic = int(subs.get("sic") or 0)
            except ValueError:
                sic = 0
            rec = (subs.get("filings") or {}).get("recent") or {}
            for e in evs:
                o = outcome(bars, e["ts"])
                if not o or o["dollar_vol"] < MIN_DOLLAR_VOLUME:
                    continue
                day = dt.date.fromisoformat(e["accepted"][:10])
                known = sorted(shares.get(cik, []))
                sh = [v for end, v in known if end <= day.isoformat()] or [v for _, v in known[:1]]
                mcap = o["ref"] * sh[-1] if sh else None
                keep = [i for i, fd in enumerate(rec.get("filingDate", [])) if fd < day.isoformat()]
                pit = {"filings": {"recent": {k: [v[i] for i in keep] for k, v in rec.items()
                                              if isinstance(v, list) and len(v) == len(rec.get("form", []))}}}
                head = bot.release_headline(e["lead"])
                text = f"{head}\n{e['lead']}"
                rows.append({
                    "ticker": ticker, "date": day.isoformat(), "ts": e["ts"], "form": e["form"], "items": e["items"],
                    "score": e["score"], "rejected": e.get("rejected"), "headline": head, "lead": e["lead"][:300],
                    "cat": bot.classify_catalyst(text) if head or e["lead"] else bot.NO_NEWS,
                    "sector": bot.sector_of(sic), "sic": sic, "mcap": mcap, "cap": bot.cap_bucket(mcap),
                    "price": bot.price_bucket(o["ref"]), "amount": bot.amount_bucket(text, mcap),
                    "pump": bot.assess_pump_risk(text, pit, day).level or "none", **o,
                })

        tickers = sorted(by_ticker)
        for i in range(0, len(tickers), 8):
            await asyncio.gather(*(one(t, by_ticker[t]) for t in tickers[i:i + 8]))
            if i % 400 == 0:
                log(f"priced {i}/{len(tickers)} tickers, {len(rows)} events with outcomes")
    return rows


# ---------------------------------------------------------------------------
# analysis


# ---------------------------------------------------------------------------


UP_TARGET = False           # "big": +10% high that holds +5% at the close (set by `report big`)
MIN_CAP = 0.0


def jumped(r: dict[str, Any]) -> bool:
    if UP_TARGET:
        return r["high2"] >= 0.10 and r["close_r"] >= 0.05
    return r["high2"] >= JUMP


def rate_table(rows: list[dict[str, Any]], key, min_n: int = 30, top: int = 25) -> list[str]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        k = key(r)
        if k:
            groups.setdefault(k, []).append(r)
    base = sum(jumped(r) for r in rows) / max(1, len(rows))
    out = [f"| קבוצה | אירועים | {'עלו 10%+ ונסגרו 5%+' if UP_TARGET else 'קפצו 20%+'} | פי מהממוצע | סגירה ב-10%+ "
           "| חציון שיא |", "|---|---|---|---|---|---|"]
    stats = []
    for k, rs in groups.items():
        if len(rs) < min_n:
            continue
        jr = sum(jumped(r) for r in rs) / len(rs)
        cr = sum(r["close_r"] >= STRONG_CLOSE for r in rs) / len(rs)
        med = sorted(r["high2"] for r in rs)[len(rs) // 2]
        stats.append((jr, k, len(rs), cr, med))
    for jr, k, n, cr, med in sorted(stats, reverse=True)[:top]:
        out.append(f"| {k} | {n} | {jr * 100:.1f}% | ×{jr / base:.1f} | {cr * 100:.1f}% | {med * 100:+.1f}% |" if base else "")
    return out


def phrase_lift(rows: list[dict[str, Any]], min_n: int = 40) -> list[tuple[float, str, int, float, float]]:
    """(lift, phrase, count, hit rate, z-score) for headline phrases, most predictive first."""
    base = sum(jumped(r) for r in rows) / max(1, len(rows))
    counts: dict[str, list[int]] = {}
    for r in rows:
        for p in bot.headline_phrases(r["headline"] or r["lead"][:160]):
            c = counts.setdefault(p, [0, 0])
            c[0] += 1
            c[1] += jumped(r)
    out = []
    for p, (n, j) in counts.items():
        if n >= min_n:
            rate = (j + base * 10) / (n + 10)        # smoothed toward the base rate
            z = (j - n * base) / math.sqrt(n * base * (1 - base)) if 0 < base < 1 else 0.0
            out.append((rate / base, p, n, j / n, z))
    return sorted(out, reverse=True)


# --- logistic model (pure Python, sparse binary features shared with the bot: bot.jump_features) ---


features = bot.jump_features


def train(rows: list[dict[str, Any]], vocab: set[str], epochs: int = 12, lr: float = 0.05, l2: float = 2e-3) -> dict[str, float]:
    w: dict[str, float] = {"bias": math.log(max(1e-3, sum(jumped(r) for r in rows) / len(rows)))}
    data = [(features(r, vocab), 1.0 if jumped(r) else 0.0) for r in rows]
    rnd = random.Random(7)
    for _ in range(epochs):
        rnd.shuffle(data)
        for feats, y in data:
            z = w["bias"] + sum(w.get(k, 0.0) for k in feats)
            p = 1 / (1 + math.exp(-max(-30, min(30, z))))
            g = p - y
            w["bias"] -= lr * g
            for k in feats:
                w[k] = w.get(k, 0.0) - lr * (g + l2 * w.get(k, 0.0))
    return w


predict = bot.jump_probability


def auc(scored: list[tuple[float, bool]]) -> float:
    pos = [s for s, y in scored if y]
    neg = [s for s, y in scored if not y]
    if not pos or not neg:
        return 0.5
    ranked = sorted(scored)
    rank_sum, i = 0.0, 0
    while i < len(ranked):
        j = i
        while j < len(ranked) and ranked[j][0] == ranked[i][0]:
            j += 1
        avg = (i + j + 1) / 2
        rank_sum += avg * sum(1 for k in range(i, j) if ranked[k][1])
        i = j
    return (rank_sum - len(pos) * (len(pos) + 1) / 2) / (len(pos) * len(neg))


def report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    OUT.mkdir(parents=True, exist_ok=True)
    rows = [r for r in rows if not r.get("rejected")]
    rows.sort(key=lambda r: r["ts"])
    n = len(rows)
    base = sum(jumped(r) for r in rows) / max(1, n)
    days = sorted({r["date"] for r in rows})
    lines = [f"# מה באמת מקפיץ מניה: {n} הודעות לעיתונות ב-SEC ({days[0]} עד {days[-1]})", "",
             ("- רק חברות של מיליארד דולר ומעלה. קפיצה = שיא של 10%+ ביום הידיעה או ביום שאחריו, וסגירה של 5%+ ביום התגובה"
              if UP_TARGET else "- קפיצה = שיא של 20%+ ביום הידיעה או ביום שאחריו")
             + ", ביחס לסגירה שלפני הידיעה. מניות שנסחרו בפחות מ-$300K ביום התגובה לא נכללו.",
             f"- **שיעור הבסיס: {base * 100:.1f}%** מכל ההודעות הקפיצו את המניה. כלומר רוב החדשות הטובות לא מזיזות כלום.",
             f"- הודעות שהבוט נתן להן 4+: {sum(1 for r in rows if r['score'] >= 4)}, מתוכן קפצו "
             f"{sum(jumped(r) for r in rows if r['score'] >= 4) * 100 // max(1, sum(1 for r in rows if r['score'] >= 4))}%", ""]
    for title, key, mn in (
        ("לפי ציון הבוט", lambda r: f"ציון {max(-1, r['score'])}", 30),
        ("לפי סוג החדשות", lambda r: r["cat"], 40),
        ("לפי שווי השוק", lambda r: r["cap"], 30),
        ("לפי מחיר המניה", lambda r: r["price"], 30),
        ("לפי סקטור", lambda r: r["sector"], 40),
        ("לפי שעת הפרסום", lambda r: {"pre": "טרום מסחר", "regular": "מסחר רגיל", "after": "אחרי המסחר",
                                       "closed": "שוק סגור"}[r["session"]], 30),
        ("לפי גודל הסכום ביחס לשווי החברה", lambda r: r["amount"], 30),
        ("לפי אזהרת פמפום (דילול / מחיקה בעבר)", lambda r: r["pump"], 30),
        ("סוג החדשות × שווי שוק (השילובים החזקים)", lambda r: f"{r['cat']} · {r['cap']}", 25),
        ("סוג החדשות × מחיר", lambda r: f"{r['cat']} · {r['price']}", 25),
        ("סוג החדשות × סקטור", lambda r: f"{r['cat']} · {r['sector']}", 25),
    ):
        lines += [f"\n## {title}\n"] + rate_table(rows, key, mn)
    lifts = phrase_lift(rows)
    lines += ["\n## מילים בכותרת שמנבאות קפיצה (מול שיעור הבסיס)\n", "| ביטוי | הופעות | קפצו | פי |", "|---|---|---|---|"]
    for lift, p, cnt, rate, z in [x for x in lifts if x[4] >= 3][:45]:
        lines.append(f"| {p} | {cnt} | {rate * 100:.0f}% | ×{lift:.1f} |")
    lines += ["\n## מילים בכותרת של חדשות 'טובות' שלא מזיזות\n", "| ביטוי | הופעות | קפצו | פי |", "|---|---|---|---|"]
    for lift, p, cnt, rate, z in [x for x in lifts if x[2] >= 150 and x[4] <= -3][-25:]:
        lines.append(f"| {p} | {cnt} | {rate * 100:.1f}% | ×{lift:.2f} |")

    # model: train on the older 75%, test on the newest 25%
    split = int(n * 0.75)
    train_rows, test_rows = rows[:split], rows[split:]
    # only phrases that are clearly not chance, measured on the training period alone
    vocab = {p for _, p, cnt, _, z in phrase_lift(train_rows, 60) if abs(z) >= 4}
    w = train(train_rows, vocab)
    scored = [(predict(w, features(r, vocab)), jumped(r)) for r in test_rows]
    model_auc = auc(scored)
    ranked = sorted(scored, key=lambda s: -s[0])
    test_base = sum(y for _, y in scored) / max(1, len(scored))
    lines += [f"\n## מודל חיזוי (אומן על {len(train_rows)} הודעות ישנות, נבדק על {len(test_rows)} חדשות)\n",
              f"- AUC: {model_auc:.3f} (0.5 = ניחוש, 1.0 = מושלם)", f"- שיעור בסיס בתקופת הבדיקה: {test_base * 100:.1f}%"]
    for frac in (0.01, 0.02, 0.05, 0.10, 0.20):
        top = ranked[:max(1, int(len(ranked) * frac))]
        rate = sum(y for _, y in top) / len(top)
        lines.append(f"- {frac * 100:.0f}% ההודעות עם הסיכוי הגבוה ביותר ({len(top)}): {rate * 100:.1f}% קפצו "
                     f"(פי {rate / test_base:.1f} מהבסיס), סף סיכוי {top[-1][0] * 100:.0f}%")
    strongest = sorted(((v, k) for k, v in w.items() if k != "bias"), reverse=True)
    lines += ["\n### המאפיינים שהכי מעלים את הסיכוי\n"] + [f"- {k} ({v:+.2f})" for v, k in strongest[:30]]
    lines += ["\n### המאפיינים שהכי מורידים\n"] + [f"- {k} ({v:+.2f})" for v, k in strongest[-15:]]
    # examples of the model's top picks in the test period
    best = sorted(test_rows, key=lambda r: -predict(w, features(r, vocab)))[:30]
    lines += ["\n### הדוגמאות שהמודל דירג הכי גבוה בתקופת הבדיקה\n", "| טיקר | יום | סיכוי | שיא | כותרת |", "|---|---|---|---|---|"]
    for r in best:
        lines.append(f"| {r['ticker']} | {r['date']} | {predict(w, features(r, vocab)) * 100:.0f}% | {r['high2'] * 100:+.0f}% | "
                     f"{(r['headline'] or r['lead'])[:80]} |")
    model = {"weights": {k: round(v, 4) for k, v in w.items() if abs(v) > 0.02}, "vocab": sorted(vocab),
             "base_rate": base, "auc": model_auc, "trained_on": len(train_rows), "jump": JUMP,
             "thresholds": {str(f): ranked[max(1, int(len(ranked) * f)) - 1][0] for f in (0.01, 0.02, 0.05, 0.10)}}
    (OUT / "model.json").write_text(json.dumps(model, ensure_ascii=False, indent=1), encoding="utf-8")
    text = "\n".join(lines) + "\n"
    (OUT / "report.md").write_text(text, encoding="utf-8")
    print(text)
    return model


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "run":
        rows = asyncio.run(collect())
        OUT.mkdir(parents=True, exist_ok=True)
        (OUT / "rows.jsonl").write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), encoding="utf-8")
        report(rows)
        return 0
    if mode == "report":
        global UP_TARGET, MIN_CAP
        rows = [json.loads(x) for x in (OUT / "rows.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
        if "big" in sys.argv[2:]:      # the bot's universe: $1B+ companies, a move that holds
            UP_TARGET, MIN_CAP = True, 1e9
            rows = [r for r in rows if (r.get("mcap") or 0) >= MIN_CAP]
        report(rows)
        return 0
    print(__doc__)
    return 2

if __name__ == "__main__":
    sys.exit(main())
