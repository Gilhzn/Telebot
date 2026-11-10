#!/usr/bin/env python3
"""Sympathy study: when a $1B+ stock opens 10%+ higher, do the stocks that usually move with it
(highest correlation of daily returns over the 60 sessions before) follow during the day?

A third of the $1B+ jumps in the live weeks had no news of their own, and many came in groups (bitcoin
miners and AI data centres on 3.11, quantum computing on 6.11). The signal is known at the open (the
leader's gap) and the trade is the peer's open, so it is measured from the peer's open:
    up   = high within two sessions >= +10% from the open and close of the day >= +5% from the open
    down = close of the day <= -5% from the open
against every $1B+ stock that opened within +-3% the same days (the base rate).

    python tools/sympathy_study.py collect 2025-08-01 2026-11-06      # on a runner, Alpaca keys + numpy
    python tools/sympathy_study.py report                               # backtest-out/sympathy/events.jsonl

Output: backtest-out/sympathy/events.jsonl (one row per leader-peer pair) and report.md
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import statistics
import sys
import time
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

OUT = Path("backtest-out/sympathy")
DATA = "https://data.alpaca.markets"
WINDOW = 60          # sessions of daily returns for the correlation
LEADER_GAP = 0.10    # the leader opens 10%+ above the prior close
PEERS = 5            # most correlated peers per leader
MIN_CORR = 0.4
MIN_DOLLAR_VOL = 5e6  # average daily $ volume over the window, both leader and peer


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


async def collect(start: str, end: str) -> None:
    key, secret = os.environ.get("ALPACA_API_KEY_ID", ""), os.environ.get("ALPACA_API_SECRET_KEY", "")
    if not key:
        raise SystemExit("ALPACA_API_KEY_ID is not set")
    auth = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    sec_ua = bot.Config.from_env().sec_user_agent or "StockNewsRadar research contact@example.com"
    end_d = min(dt.date.fromisoformat(end), dt.date.today() - dt.timedelta(days=1))
    OUT.mkdir(parents=True, exist_ok=True)
    async with httpx.AsyncClient(timeout=30) as client:
        tickers = bot.TickerMap()
        tickers.load((await client.get(bot.TICKERS_EXCHANGE_URL, headers={"User-Agent": sec_ua})).json())
        universe = bot.research_universe(tickers)
        log(f"universe: {len(universe)} listed common stocks")
        bars: dict[str, dict[dt.date, tuple[float, float, float, float, float]]] = {}
        for i in range(0, len(universe), 100):
            alp = {s.replace("-", "."): s for s in universe[i:i + 100]}
            token = None
            while True:
                params = {"symbols": ",".join(alp), "timeframe": "1Day", "start": start, "end": end_d.isoformat(),
                          "adjustment": "split", "feed": "sip", "limit": 10000}
                if token:
                    params["page_token"] = token
                for attempt in range(5):
                    r = await client.get(f"{DATA}/v2/stocks/bars", params=params, headers=auth)
                    if r.status_code != 429 and r.status_code < 500:
                        break
                    await asyncio.sleep(5 * (attempt + 1))
                if r.status_code != 200:
                    log(f"bars {r.status_code}: {r.text[:150]}")
                    break
                data = r.json()
                for sym, rows in (data.get("bars") or {}).items():
                    for b in rows:
                        d = dt.datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(bot.eastern_tz()).date()
                        bars.setdefault(alp.get(sym, sym), {})[d] = (b["o"], b["h"], b["l"], b["c"], b["v"])
                token = data.get("next_page_token")
                if not token:
                    break
                await asyncio.sleep(0.31)
            if i % 1000 == 0:
                log(f"bars: {i} symbols requested, {len(bars)} with data")
            await asyncio.sleep(0.31)
        shares: dict[int, list[tuple[str, float]]] = {}
        y0 = dt.date.fromisoformat(start).year
        for year in range(y0, end_d.year + 1):
            for q in (1, 2, 3, 4):
                r = await client.get(bot.SEC_FRAMES_URL.format(period=f"CY{year}Q{q}I"), headers={"User-Agent": sec_ua})
                if r.status_code == 200:
                    for x in r.json().get("data", []):
                        shares.setdefault(int(x["cik"]), []).append((x["end"], float(x["val"])))
                await asyncio.sleep(0.2)
    log(f"bars for {len(bars)} symbols, share counts for {len(shares)} companies")
    rows, meta = compute(bars, tickers, shares)
    (OUT / "events.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    (OUT / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    log(f"{len(rows)} leader-peer pairs")
    report(rows, meta)


def compute(bars: dict[str, dict[dt.date, tuple]], tickers: Any,
            shares: dict[int, list[tuple[str, float]]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    import numpy as np

    dates = sorted({d for b in bars.values() for d in b})
    syms = sorted(bars)
    n, m = len(dates), len(syms)
    di = {d: i for i, d in enumerate(dates)}
    O, H, C, V = (np.full((n, m), np.nan) for _ in range(4))
    for j, s in enumerate(syms):
        for d, (o, h, _l, c, v) in bars[s].items():
            i = di[d]
            O[i, j], H[i, j], C[i, j], V[i, j] = o, h, c, v
    sh = np.full(m, np.nan)
    for j, s in enumerate(syms):
        found = tickers.lookup(s)
        known = sorted(shares.get(found[0], [])) if found else []
        if known:
            sh[j] = known[-1][1]            # latest share count: size at the end of the period is close enough
    prev_c = np.vstack([np.full((1, m), np.nan), C[:-1]])
    ret = C / prev_c - 1
    gap = O / prev_c - 1
    mcap = prev_c * sh
    dvol = C * V
    nxt_h = np.vstack([H[1:], np.full((1, m), np.nan)])
    high2 = np.fmax(H, nxt_h) / O - 1
    close_o = C / O - 1
    rows: list[dict[str, Any]] = []
    base_up = base_dn = base_n = 0
    for i in range(WINDOW + 1, n - 1):
        w = ret[i - WINDOW:i]
        ok = (~np.isnan(w)).sum(axis=0) >= WINDOW - 3
        avg_dv = np.nanmean(dvol[i - WINDOW:i], axis=0)
        big = ok & (mcap[i] >= 1e9) & (avg_dv >= MIN_DOLLAR_VOL) & (prev_c[i] >= 3) & ~np.isnan(O[i])
        # base rate: every $1B+ stock opening within +-3%
        calm = big & (np.abs(gap[i]) < 0.03) & ~np.isnan(high2[i])
        base_n += int(calm.sum())
        base_up += int(((high2[i] >= 0.10) & (close_o[i] >= 0.05) & calm).sum())
        base_dn += int(((close_o[i] <= -0.05) & calm).sum())
        leaders = np.where(big & (gap[i] >= LEADER_GAP) & (gap[i] < 1.0))[0]
        if not len(leaders):
            continue
        wz = np.where(np.isnan(w), 0.0, w)
        wz = wz - wz.mean(axis=0)
        sd = wz.std(axis=0)
        sd[sd == 0] = np.nan
        z = wz / sd
        n_leaders = len(leaders)
        for j in leaders:
            corr = (z[:, j] @ z) / WINDOW
            corr[~big] = np.nan
            corr[j] = np.nan
            order = [k for k in np.argsort(-np.nan_to_num(corr, nan=-9))[:PEERS] if corr[k] >= MIN_CORR]
            for k in order:
                if np.isnan(high2[i, k]):
                    continue
                rows.append({
                    "day": dates[i].isoformat(), "leader": syms[j], "peer": syms[k], "corr": round(float(corr[k]), 3),
                    "leader_gap": round(float(gap[i, j]), 4), "leader_close": round(float(C[i, j] / prev_c[i, j] - 1), 4),
                    "leader_mcap": float(mcap[i, j]), "peer_mcap": float(mcap[i, k]), "peer_gap": round(float(gap[i, k]), 4),
                    "peer_pre5": round(float(C[i - 1, k] / C[i - 6, k] - 1), 4) if i >= 6 and C[i - 6, k] > 0 else None,
                    "high2": round(float(high2[i, k]), 4), "close_o": round(float(close_o[i, k]), 4),
                    "n_leaders": n_leaders, "rank": order.index(k) + 1,
                })
    return rows, {"base_n": base_n, "base_up": base_up, "base_down": base_dn, "days": n, "symbols": m}


def up(r: dict[str, Any]) -> bool:
    return r["high2"] >= 0.10 and r["close_o"] >= 0.05


def down(r: dict[str, Any]) -> bool:
    return r["close_o"] <= -0.05


def report(rows: list[dict[str, Any]], meta: dict[str, Any]) -> None:
    lines = [f"# Sympathy: peers of a $1B+ stock that opens 10%+ higher ({len(rows)} pairs)", "",
             "measured from the peer's open: up = +10% high within two sessions and +5% at the close; "
             "down = -5% at the close", "",
             f"base rate (every $1B+ stock opening within +-3%, {meta['base_n']} stock-days): "
             f"up {meta['base_up'] / max(1, meta['base_n']) * 100:.2f}%, down {meta['base_down'] / max(1, meta['base_n']) * 100:.2f}%",
             "", "| group | n | up | down | median close from open |", "|---|---|---|---|---|"]

    def row(name: str, rs: list[dict[str, Any]], min_n: int = 30) -> None:
        if len(rs) >= min_n:
            lines.append(f"| {name} | {len(rs)} | {sum(map(up, rs)) / len(rs) * 100:.1f}% | "
                         f"{sum(map(down, rs)) / len(rs) * 100:.1f}% | {statistics.median(r['close_o'] for r in rs) * 100:+.1f}% |")

    row("all pairs", rows)
    for lo, hi in ((-1, 0), (0, 0.03), (0.03, 0.08), (0.08, 9)):
        row(f"peer gap {lo:+.0%}..{hi:+.0%}", [r for r in rows if lo <= r["peer_gap"] < hi])
    calm = [r for r in rows if -0.03 < r["peer_gap"] < 0.03]
    row("peer not moved yet (gap within +-3%)", calm)
    for lo, hi in ((0.4, 0.5), (0.5, 0.6), (0.6, 0.7), (0.7, 1.01)):
        row(f"  corr {lo:.1f}-{hi:.1f}", [r for r in calm if lo <= r["corr"] < hi])
    for lo, hi in ((0.10, 0.15), (0.15, 0.25), (0.25, 1.0)):
        row(f"  leader gap {lo:.0%}-{hi:.0%}", [r for r in calm if lo <= r["leader_gap"] < hi])
    row("  leader holds (closes 10%+ up)", [r for r in calm if r["leader_close"] >= 0.10])
    row("  leader fades (closes under +5%)", [r for r in calm if r["leader_close"] < 0.05])
    for a, b in ((1, 1), (2, 3), (4, 9), (10, 999)):
        row(f"  leaders that day {a}-{b}", [r for r in calm if a <= r["n_leaders"] <= b])
    row("  leader bigger than the peer", [r for r in calm if r["leader_mcap"] > r["peer_mcap"]])
    row("  leader smaller than the peer", [r for r in calm if r["leader_mcap"] <= r["peer_mcap"]])
    for lo, hi, nm in ((1e9, 2e9, "1-2B"), (2e9, 10e9, "2-10B"), (10e9, 1e16, "10B+")):
        row(f"  peer {nm}", [r for r in calm if lo <= r["peer_mcap"] < hi])
    row("  peer rank 1", [r for r in calm if r["rank"] == 1])
    row("  peer up 10%+ in the 5 sessions before", [r for r in calm if (r["peer_pre5"] or 0) >= 0.10])
    best = [r for r in calm if r["corr"] >= 0.6 and r["leader_gap"] >= 0.15]
    row("  corr 0.6+ and leader gap 15%+", best)
    # by peer: several leaders in the same group on one day count once
    lines += ["", "## a sample of peer-days that rose (corr, leader -> peer)", ""]
    for r in [r for r in calm if up(r)][:: max(1, sum(map(up, calm)) // 40)][:40]:
        lines.append(f"- {r['day']} {r['leader']} {r['leader_gap'] * 100:+.0f}% -> {r['peer']} "
                     f"(corr {r['corr']:.2f}) high {r['high2'] * 100:+.0f}%, close {r['close_o'] * 100:+.0f}% from open")
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
        report(rows, json.loads((OUT / "meta.json").read_text(encoding="utf-8")))
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
