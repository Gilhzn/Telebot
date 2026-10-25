#!/usr/bin/env python3
"""Earnings study: which results headlines move a $1B+ stock, and by how much.

Benzinga (through Alpaca's news API, the same feed the bot streams) writes a headline for almost
every US company's results within seconds, e.g. "Acme Q3 Adj. EPS $0.28 Beats $0.19 Estimate,
Sales $5.6B Miss $5.7B Estimate", and separate ones for guidance ("Acme Raises FY26 EPS Guidance
...", "Acme Sees Q4 Sales ... vs ... Est"). For a year of them this collects every such headline
and the stock's reaction (Alpaca daily bars) and size (SEC share counts), so the bot can learn which
surprise is worth an alert.

    python tools/earnings_study.py collect 2025-10-01 2026-10-24    # on a runner, needs Alpaca keys
    python tools/earnings_study.py report                            # backtest-out/earnings/events.jsonl

Output: backtest-out/earnings/events.jsonl (one row per company per results day) and report.md
"""
from __future__ import annotations

import asyncio
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

OUT = Path("backtest-out/earnings")
DATA = "https://data.alpaca.markets"
KEEP_RE = re.compile(r"\bEPS\b|\bSales\b.*\bEst|\bGuidance\b|\bOutlook\b|\bSees\b|\bRaises\b|\bLowers\b|\bCuts\b", re.I)
NUM = r"\(?-?\$?\(?-?[\d,]*\.?\d+\)?"
EPS_RE = re.compile(rf"\bEPS\s+(?P<a>{NUM})\s+(?P<v>Beats|Misses|In-Line|Up|Down|Vs\.?)\s*(?:With\s+)?(?P<e>{NUM})?", re.I)
SALES_RE = re.compile(rf"\b(?:Sales|Revenue)\s+(?P<a>{NUM})\s*(?P<au>[KMB])?\s+(?P<v>Beat|Beats|Miss|Misses|In-Line|Up|Down|Vs\.?)"
                      rf"\s*(?:With\s+)?(?P<e>{NUM})?\s*(?P<eu>[KMB])?", re.I)
GUIDE_UP_RE = re.compile(r"\b(?:Raises|Boosts|Lifts|Increases|Ups)\b.{0,40}\b(?:Guidance|Outlook|Forecast|Sees)|"
                         r"\bSees\b.{0,80}\b(?:Above|Ahead of|Vs)\b.{0,40}\bEst", re.I)
GUIDE_DOWN_RE = re.compile(r"\b(?:Lowers|Cuts|Reduces|Trims|Withdraws)\b.{0,40}\b(?:Guidance|Outlook|Forecast)|"
                           r"\bSees\b.{0,80}\bBelow\b", re.I)


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def money(s: str | None) -> float | None:
    if not s:
        return None
    neg = "(" in s or s.strip().startswith("-") or "$-" in s
    try:
        v = float(re.sub(r"[^\d.]", "", s))
    except ValueError:
        return None
    return -v if neg else v


def parse_headlines(heads: list[str]) -> dict[str, Any]:
    """EPS / sales surprise and guidance direction from one company's results headlines."""
    out: dict[str, Any] = {}
    for h in heads:
        m = EPS_RE.search(h)
        if m and "eps_a" not in out:
            a, e = money(m.group("a")), money(m.group("e"))
            out["eps_a"], out["eps_e"] = a, e
            if a is not None and e is not None:
                out["eps_s"] = (a - e) / max(abs(e), 0.05)
        m = SALES_RE.search(h)
        if m and "sales_a" not in out:
            unit = {"K": 1e3, "M": 1e6, "B": 1e9}
            a, e = money(m.group("a")), money(m.group("e"))
            if a is not None and e:
                a *= unit.get((m.group("au") or "").upper(), 1)
                e *= unit.get((m.group("eu") or m.group("au") or "").upper(), 1)
                out["sales_s"] = a / e - 1 if e else None
        if GUIDE_UP_RE.search(h):
            out["guide"] = "up" if out.get("guide") in (None, "up") else "mixed"
        if GUIDE_DOWN_RE.search(h):
            out["guide"] = "down" if out.get("guide") in (None, "down") else "mixed"
    return out


# ---------------------------------------------------------------------------
# collect (runner)
# ---------------------------------------------------------------------------


async def collect(start: str, end: str) -> None:
    key, secret = os.environ.get("ALPACA_API_KEY_ID", ""), os.environ.get("ALPACA_API_SECRET_KEY", "")
    if not key:
        raise SystemExit("ALPACA_API_KEY_ID is not set")
    auth = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    OUT.mkdir(parents=True, exist_ok=True)
    heads_file = OUT / "headlines.jsonl"
    heads: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=30) as client:
        async def get(url: str, params: dict[str, Any], headers: dict[str, str]) -> httpx.Response:
            for attempt in range(6):
                r = await client.get(url, params=params, headers=headers)
                if r.status_code == 429 or r.status_code >= 500:
                    await asyncio.sleep(5 * (attempt + 1))
                    continue
                return r
            r.raise_for_status()
            return r

        # 1. every results / guidance headline (one company each)
        token, pages, seen = None, 0, 0
        while True:
            params = {"start": f"{start}T00:00:00Z", "end": f"{end}T23:59:59Z", "limit": 50, "sort": "asc",
                      "include_content": "false"}
            if token:
                params["page_token"] = token
            r = await get(f"{DATA}/v1beta1/news", params, auth)
            r.raise_for_status()
            data = r.json()
            for n in data.get("news", []):
                seen += 1
                syms = n.get("symbols") or []
                if len(syms) == 1 and KEEP_RE.search(n.get("headline", "")):
                    heads.append({"symbol": syms[0], "t": n["created_at"], "headline": n["headline"]})
            pages += 1
            token = data.get("next_page_token")
            if pages % 200 == 0:
                log(f"news pages {pages}, items {seen}, kept {len(heads)}, at {data['news'][-1]['created_at'] if data.get('news') else '?'}")
            if not token:
                break
            await asyncio.sleep(0.32)            # 200 requests a minute
        heads_file.write_text("".join(json.dumps(h) + "\n" for h in heads), encoding="utf-8")
        log(f"{len(heads)} results/guidance headlines out of {seen} news items")

        # 2. group by company and US day; keep days with an EPS or sales headline
        events: dict[tuple[str, str], dict[str, Any]] = {}
        for h in heads:
            ts = dt.datetime.fromisoformat(h["t"].replace("Z", "+00:00")).timestamp()
            day = dt.datetime.fromtimestamp(ts, bot.eastern_tz())
            # results after the close belong with the next morning's headlines
            key_day = (day + dt.timedelta(days=1)).date() if day.time() >= dt.time(16, 0) else day.date()
            ev = events.setdefault((h["symbol"], key_day.isoformat()), {"symbol": h["symbol"], "ts": ts, "heads": []})
            ev["ts"] = min(ev["ts"], ts)
            ev["heads"].append(h["headline"])
        events = {k: v for k, v in events.items() if any(EPS_RE.search(x) or SALES_RE.search(x) for x in v["heads"])}
        symbols = sorted({k[0] for k in events})
        log(f"{len(events)} results events for {len(symbols)} companies")

        # 3. daily bars (split-adjusted, all US trades)
        bars: dict[str, list[tuple[dt.date, float, float, float, float, float]]] = {}
        d0 = (dt.date.fromisoformat(start) - dt.timedelta(days=15)).isoformat()
        for i in range(0, len(symbols), 100):
            token = None
            while True:
                params = {"symbols": ",".join(symbols[i:i + 100]), "timeframe": "1Day", "start": d0, "end": end,
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
                        bars.setdefault(sym, []).append((d, b["o"], b["h"], b["l"], b["c"], b["v"]))
                token = data.get("next_page_token")
                if not token:
                    break
                await asyncio.sleep(0.32)
            await asyncio.sleep(0.32)
        log(f"bars for {len(bars)} companies")

        # 4. shares outstanding (SEC frames) for market cap
        sec_ua = bot.Config.from_env().sec_user_agent or "research contact@example.com"
        tickers = (await client.get(bot.TICKERS_EXCHANGE_URL, headers={"User-Agent": sec_ua})).json()
        f = {name: i for i, name in enumerate(tickers["fields"])}
        cik_of = {row[f["ticker"]].replace("-", "."): int(row[f["cik"]]) for row in tickers["data"]}
        shares: dict[int, list[tuple[str, float]]] = {}
        for year, q in ((2025, 3), (2025, 4), (2026, 1), (2026, 2), (2026, 3)):
            r = await client.get(bot.SEC_FRAMES_URL.format(period=f"CY{year}Q{q}I"), headers={"User-Agent": sec_ua})
            if r.status_code == 200:
                for x in r.json().get("data", []):
                    shares.setdefault(int(x["cik"]), []).append((x["end"], float(x["val"])))
            await asyncio.sleep(0.2)

    rows = []
    for (sym, day), ev in events.items():
        o = outcome(sorted(bars.get(sym, [])), ev["ts"])
        if not o:
            continue
        cik = cik_of.get(sym)
        known = sorted(shares.get(cik, [])) if cik else []
        sh = [v for end_, v in known if end_ <= day] or [v for _, v in known[:1]]
        rows.append({"symbol": sym, "day": day, "ts": ev["ts"], "heads": ev["heads"][:8],
                     "mcap": o["ref"] * sh[-1] if sh else None, **parse_headlines(ev["heads"]), **o})
    (OUT / "events.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    log(f"{len(rows)} events with prices")
    report(rows)


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------


def up(r: dict[str, Any]) -> bool:
    return r["high2"] >= 0.10 and r["close_r"] >= 0.05


def down(r: dict[str, Any]) -> bool:
    return r["close_r"] <= -0.05


def report(rows: list[dict[str, Any]]) -> None:
    big = [r for r in rows if (r.get("mcap") or 0) >= 1e9]
    lines = [f"# Earnings headlines and the reaction, $1B+ companies ({len(big)} results days)", "",
             "up = +10% high within two days and +5% at the reaction day's close; down = -5% close", "",
             "| group | n | up | down | median close |", "|---|---|---|---|---|"]

    def row(name: str, rs: list[dict[str, Any]]) -> None:
        if len(rs) >= 15:
            lines.append(f"| {name} | {len(rs)} | {sum(map(up, rs)) / len(rs) * 100:.1f}% | "
                         f"{sum(map(down, rs)) / len(rs) * 100:.1f}% | {statistics.median(r['close_r'] for r in rs) * 100:+.1f}% |")

    row("all", big)
    for lo, hi in ((-9, -0.5), (-0.5, -0.1), (-0.1, 0), (0, 0.05), (0.05, 0.15), (0.15, 0.3), (0.3, 0.6), (0.6, 99)):
        row(f"EPS surprise {lo:+.0%}..{hi:+.0%}", [r for r in big if r.get("eps_s") is not None and lo <= r["eps_s"] < hi])
    for lo, hi in ((-9, -0.02), (-0.02, 0), (0, 0.02), (0.02, 0.05), (0.05, 99)):
        row(f"sales surprise {lo:+.0%}..{hi:+.0%}", [r for r in big if r.get("sales_s") is not None and lo <= r["sales_s"] < hi])
    for g in ("up", "down", "mixed", None):
        row(f"guidance {g or 'none'}", [r for r in big if r.get("guide") == g])
    both = [r for r in big if (r.get("eps_s") or 0) >= 0.1 and (r.get("sales_s") or 0) >= 0.02]
    row("EPS +10%+ and sales +2%+", both)
    row("EPS +10%+, sales +2%+, guidance up", [r for r in both if r.get("guide") == "up"])
    row("EPS +10%+, sales +2%+, no guidance cut", [r for r in both if r.get("guide") not in ("down", "mixed")])
    row("guidance up, any EPS", [r for r in big if r.get("guide") == "up"])
    row("guidance up and sales beat", [r for r in big if r.get("guide") == "up" and (r.get("sales_s") or 0) > 0])
    row("sales miss, guidance down", [r for r in big if (r.get("sales_s") or 0) < 0 and r.get("guide") == "down"])
    for lo, hi, nm in ((1e9, 2e9, "1-2B"), (2e9, 10e9, "2-10B"), (10e9, 1e15, "10B+")):
        sub = [r for r in big if lo <= r["mcap"] < hi]
        row(f"{nm}: EPS +10%+ & sales +2%+ & guidance up", [r for r in sub if r in both and r.get("guide") == "up"])
        row(f"{nm}: all", sub)
    unparsed = [r for r in big if r.get("eps_s") is None and r.get("sales_s") is None]
    lines += ["", f"unparsed results days: {len(unparsed)}; sample headlines:"]
    lines += [f"- {r['heads'][0][:140]}" for r in unparsed[:25]]
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
        rows = [json.loads(x) for x in (OUT / "events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
        report(rows)
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
