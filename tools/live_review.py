#!/usr/bin/env python3
"""Live scorecard for the weekly review: how the alerts the bot really pushed did, measured the same
way as the yearly formula study (catalysts.outcome: +10% high within two days and +5% at the close
of the reaction day, both against the close before the news), and which $1B+ gainers the bot saw.

    python tools/live_review.py state.json 2026-11-01      # on a runner, needs the Alpaca keys

Output: backtest-out/live/alerts.jsonl, gainers.jsonl and report.md
"""
from __future__ import annotations

import asyncio
import collections
import datetime as dt
import json
import os
import re
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

OUT = Path("backtest-out/live")
DATA = "https://data.alpaca.markets"
TAKEOVER_WORDS = re.compile(r"acquir|merger|takeover|buyout|tender offer|to buy\b|\boffer for\b", re.I)


def kind(a: dict[str, Any]) -> str:
    """Which path of the formula an alert came from (from what the alert log keeps)."""
    t, src = a.get("title") or "", a.get("src") or ""
    if src == bot.ALPACA_SOURCE and (bot.EPS_HEAD_RE.search(t) or bot.SALES_HEAD_RE.search(t)
                                     or bot.GUIDE_RANGE_RE.search(t) or bot.GUIDE_WORD_UP_RE.search(t)):
        return "results numbers (Benzinga)"
    if src.startswith("SEC") and re.fullmatch(r"[\d., ]+", t) and "2.02" in t:
        return "results 8-K (no headline)"
    if src.startswith("SEC") and re.fullmatch(r"[\d., ]+|6-K|8-K", t):
        return "8-K/6-K other (no headline)"
    if TAKEOVER_WORDS.search(t):
        return "takeover / deal"
    if bot.EARNINGS_RE.search(t):
        return "results release (wire)"
    if bot.CONTRACT_RE.search(t):
        return "contract / order"
    if re.search(r"\bFDA\b|phase|trial|topline|data\b|study", t, re.I):
        return "trial / FDA"
    return "other"


def up(o: dict[str, Any]) -> bool:
    return o["high2"] >= 0.10 and o["close_r"] >= 0.05


def down(o: dict[str, Any]) -> bool:
    return o["close_r"] <= -0.05


def auth() -> dict[str, str]:
    key, secret = os.environ.get("ALPACA_API_KEY_ID", ""), os.environ.get("ALPACA_API_SECRET_KEY", "")
    if not key:
        raise SystemExit("ALPACA_API_KEY_ID is not set")
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


async def benzinga_texts(alerts: list[dict[str, Any]]) -> dict[int, str]:
    """The summary Benzinga sent with each alerted headline (the alert log keeps only the title), so the
    review can see which words the rules scored."""
    out: dict[int, str] = {}
    async with httpx.AsyncClient(timeout=30) as client:
        for i, a in enumerate(alerts):
            if a.get("src") != bot.ALPACA_SOURCE:
                continue
            t0 = dt.datetime.fromtimestamp(a["t"] - 900, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            t1 = dt.datetime.fromtimestamp(a["t"] + 60, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            r = await client.get(f"{DATA}/v1beta1/news", headers=auth(), params={
                "symbols": a["ticker"].replace("-", "."), "start": t0, "end": t1, "limit": 50, "include_content": "false"})
            if r.status_code == 200:
                head = (a.get("title") or "")[:60]
                for n in r.json().get("news", []):
                    if n.get("headline", "")[:60] == head:
                        out[i] = n.get("summary") or ""
                        break
            await asyncio.sleep(0.35)
    return out


async def benzinga_heads(gainers: list[dict[str, Any]]) -> list[list[str]]:
    """Benzinga's results / guidance headlines for each gainer in the 20 hours before its move began."""
    out: list[list[str]] = []
    async with httpx.AsyncClient(timeout=30) as client:
        for g in gainers:
            t0 = dt.datetime.fromtimestamp(g["start"] - 20 * 3600, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            t1 = dt.datetime.fromtimestamp(g["start"], dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            hs: list[str] = []
            r = await client.get(f"{DATA}/v1beta1/news", headers=auth(), params={
                "symbols": g["ticker"].replace("-", "."), "start": t0, "end": t1, "limit": 50, "include_content": "false"})
            if r.status_code == 200:
                for n in r.json().get("news", []):
                    h = n.get("headline", "")
                    if len(n.get("symbols") or []) <= 3 and (bot.EPS_HEAD_RE.search(h) or bot.SALES_HEAD_RE.search(h)
                                                              or bot.GUIDE_RANGE_RE.search(h) or bot.GUIDE_WORD_UP_RE.search(h)):
                        hs.append(h)
            out.append(hs)
            await asyncio.sleep(0.35)
    return out


async def daily_bars(symbols: list[str], start: str) -> dict[str, list[tuple]]:
    auth_ = auth()
    alp = {s: s.replace("-", ".") for s in symbols}
    back = {v: k for k, v in alp.items()}
    bars: dict[str, list[tuple]] = {}
    async with httpx.AsyncClient(timeout=30) as client:
        for i in range(0, len(symbols), 100):
            token = None
            while True:
                params = {"symbols": ",".join(alp[s] for s in symbols[i:i + 100]), "timeframe": "1Day",
                          "start": start, "adjustment": "split", "feed": "sip", "limit": 10000}
                if token:
                    params["page_token"] = token
                r = await client.get(f"{DATA}/v2/stocks/bars", params=params, headers=auth_)
                if r.status_code != 200:
                    print(f"bars {r.status_code}: {r.text[:150]}")
                    break
                data = r.json()
                for sym, rows in (data.get("bars") or {}).items():
                    for b in rows:
                        d = dt.datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(bot.eastern_tz()).date()
                        bars.setdefault(back.get(sym, sym), []).append((d, b["o"], b["h"], b["l"], b["c"], b["v"]))
                token = data.get("next_page_token")
                if not token:
                    break
                await asyncio.sleep(0.35)
    return {k: sorted(v) for k, v in bars.items()}


def pct(n: int, d: int) -> str:
    return f"{n / d * 100:.0f}%" if d else "—"


def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    state = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    since = dt.datetime.fromisoformat(sys.argv[2]).replace(tzinfo=bot.eastern_tz()).timestamp()
    alerts, seen = [], set()
    for a in sorted(state.get("alert_log", []), key=lambda a: a["t"]):
        if a["t"] < since or not a.get("ticker"):
            continue
        o_day = dt.datetime.fromtimestamp(a["t"], bot.eastern_tz())
        day = (o_day + dt.timedelta(days=1)).date() if o_day.time() >= dt.time(16, 0) else o_day.date()
        if (a["ticker"], day) in seen:      # one alert per company per reaction day, the first one
            continue
        seen.add((a["ticker"], day))
        alerts.append(a)
    gainers = [g for g in state.get("gainers_log", []) if g.get("large") and g.get("date", "") >= sys.argv[2]]
    symbols = sorted({a["ticker"] for a in alerts} | {g["ticker"] for g in gainers})
    start = (dt.date.fromisoformat(sys.argv[2]) - dt.timedelta(days=12)).isoformat()
    bars = asyncio.run(daily_bars(symbols, start))
    summaries = asyncio.run(benzinga_texts(alerts))
    OUT.mkdir(parents=True, exist_ok=True)

    rows = []
    for i, a in enumerate(alerts):
        o = outcome(bars.get(a["ticker"], []), a["t"])
        title, summary = a.get("title") or "", summaries.get(i, "")
        rules = bot.rule_score(f"{title}\n{summary}", strong_text=f"{title}\n{summary[:bot.STRONG_CHARS]}", title=title)
        rows.append({**{k: a.get(k) for k in ("t", "ticker", "score", "src", "title", "jump", "pump")},
                     "kind": kind(a), "summary": summary[:400], "labels": rules.labels, **({"o": o} if o else {})})
    (OUT / "alerts.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    done = [r for r in rows if r.get("o")]

    lines = [f"# Live alerts since {sys.argv[2]} ({len(rows)} company-days, {len(done)} with a reaction day)", "",
             "up = +10% high within two days and +5% at the reaction day's close; down = -5% close; "
             "both vs the close before the news (the yearly study's measure)", "",
             "| group | n | up | down | median close |", "|---|---|---|---|---|"]

    def row(name: str, rs: list[dict[str, Any]]) -> None:
        if rs:
            lines.append(f"| {name} | {len(rs)} | {pct(sum(up(r['o']) for r in rs), len(rs))} | "
                         f"{pct(sum(down(r['o']) for r in rs), len(rs))} | "
                         f"{statistics.median(r['o']['close_r'] for r in rs) * 100:+.1f}% |")

    row("all", done)
    for k in sorted({r["kind"] for r in done}):
        row(k, [r for r in done if r["kind"] == k])
    for s in (5, 4):
        row(f"score {s}", [r for r in done if r["score"] == s])
    by_day = collections.defaultdict(list)
    for r in done:
        by_day[dt.datetime.fromtimestamp(r["t"], bot.eastern_tz()).strftime("%a %m-%d")].append(r)
    for d in sorted(by_day, key=lambda x: x[4:]):
        row(f"alerted {d}", by_day[d])
    lines += ["", "## each alert", ""]
    for r in sorted(done, key=lambda r: -r["o"]["close_r"]):
        o = r["o"]
        mark = "UP" if up(o) else "DOWN" if down(o) else ""
        lines.append(f"- {mark:4} {r['ticker']} s{r['score']} close {o['close_r'] * 100:+.1f}% high2 {o['high2'] * 100:+.1f}% "
                     f"[{r['kind']}] {(r['title'] or '')[:90]}" + (f" — rules: {', '.join(r['labels'][:3])}" if r.get("labels") else ""))

    # $1B+ gainers: did an alert come before the move?
    by_ticker = collections.defaultdict(list)
    for a in state.get("alert_log", []):
        by_ticker[a["ticker"]].append(a)
    g_rows = []
    for g in gainers:
        start_t = g.get("start") or 0
        near = [a for a in by_ticker.get(g["ticker"], []) if abs(start_t - a["t"]) < 86400]
        first = min(near, key=lambda a: a["t"]) if near else None
        g_rows.append({**g, "alert_lead_min": round((start_t - first["t"]) / 60) if first and start_t else None})
    # for each gainer the bot did not alert: did Benzinga write a results headline before the move, and
    # what did the earnings table make of it? (missed headline = source gap, low score = below the bar)
    missed = [g for g in g_rows if g["alert_lead_min"] is None and g.get("start")]
    heads = asyncio.run(benzinga_heads(missed))
    for g, hs in zip(missed, heads):
        sig = bot.earnings_signal(hs) if hs else {}
        sc = bot.earnings_score(sig) if hs else None
        g["bz_results"] = hs[:4]
        g["bz_score"] = sc[0] if sc else None
        g["bz_reason"] = sc[1] if sc else ""
    (OUT / "gainers.jsonl").write_text("".join(json.dumps(r) + "\n" for r in g_rows), encoding="utf-8")
    caught = [g for g in g_rows if g["alert_lead_min"] is not None]
    before = [g for g in caught if g["alert_lead_min"] >= 0]
    unseen = [g for g in g_rows if g["alert_lead_min"] is None and g.get("missing_source")]
    low = [g for g in g_rows if g["alert_lead_min"] is None and not g.get("missing_source") and g.get("title")]
    nonews = [g for g in g_rows if not g.get("title")]
    lines += ["", f"## $1B+ gainers ({len(g_rows)})", "",
              f"alerted {len(caught)} (before the move {len(before)}), missed with news the bot did not see "
              f"{len(unseen)}, seen but scored low {len(low)}, no public news {len(nonews)}", ""]
    for g in sorted(g_rows, key=lambda g: (g["date"], -g["pct"])):
        tag = (f"alert {g['alert_lead_min']:+d}m" if g["alert_lead_min"] is not None else
               "no news" if not g.get("title") else "unseen" if g.get("missing_source") else f"low score {g.get('rule_score')}")
        lines.append(f"- {g['date']} {g['ticker']} +{g['pct']:.0f}% ({tag}) [{g.get('cat') or ''}] "
                     f"{(g.get('title') or '')[:90]}")
        if g.get("bz_results"):
            lines.append(f"    - Benzinga results headlines before the move (score {g['bz_score']}: {g['bz_reason'][:80]}): "
                         + " | ".join(h[:80] for h in g["bz_results"][:2]))
    with_heads = [g for g in g_rows if g.get("bz_results")]
    lines += ["", f"missed gainers with a Benzinga results headline before the move: {len(with_heads)} "
              f"(scored 4+ by the earnings table, i.e. the bot should have alerted: "
              f"{sum(1 for g in with_heads if (g.get('bz_score') or 0) >= 4)})"]
    text = "\n".join(lines) + "\n"
    (OUT / "report.md").write_text(text, encoding="utf-8")
    print(text)
    print(f"done {time.strftime('%H:%M:%S')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
