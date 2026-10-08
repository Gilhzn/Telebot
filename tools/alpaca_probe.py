"""Alpaca check from a runner: are the keys set and accepted, which data do they open (news,
pre-market snapshots, market movers), and a race between Alpaca's real-time news stream
(Benzinga) and the bot's own sources. Never prints the keys.

    python tools/alpaca_probe.py 900     # race for 15 minutes
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

import feedparser
import httpx
import websockets

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

DURATION = float(sys.argv[1]) if len(sys.argv) > 1 else 900.0
KEY, SECRET = os.environ.get("ALPACA_API_KEY_ID", "").strip(), os.environ.get("ALPACA_API_SECRET_KEY", "").strip()
AUTH = {"APCA-API-KEY-ID": KEY, "APCA-API-SECRET-KEY": SECRET}
DATA = "https://data.alpaca.markets"
STREAM = "wss://stream.data.alpaca.markets/v1beta1/news"
WIRES = {
    "PRN page": bot.PRN_LIST_URL,
    "Globe rss": next(u for u in bot._split(bot.DEFAULT_WIRE_FEEDS) if "orgclass" in u),
    "BW rss": "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtRWA==",
}


def norm(title: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", title.lower())[:9])


def et(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, bot.eastern_tz()).strftime("%H:%M:%S")


def iso_ts(s: str) -> float:
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


async def rest_checks(client: httpx.AsyncClient) -> None:
    print(f"keys: ALPACA_API_KEY_ID {'set' if KEY else 'MISSING'} ({len(KEY)} chars, starts {KEY[:2]!r}), "
          f"ALPACA_API_SECRET_KEY {'set' if SECRET else 'MISSING'} ({len(SECRET)} chars)")
    for name, url in (
        ("news (latest 10)", f"{DATA}/v1beta1/news?limit=10"),
        ("snapshots iex", f"{DATA}/v2/stocks/snapshots?symbols=AAPL,TSLA,OLB,BIAF,PCRX&feed=iex"),
        ("snapshots sip", f"{DATA}/v2/stocks/snapshots?symbols=AAPL,TSLA,OLB,BIAF,PCRX&feed=sip"),
        ("snapshots delayed_sip", f"{DATA}/v2/stocks/snapshots?symbols=AAPL,OLB&feed=delayed_sip"),
        ("movers", f"{DATA}/v1beta1/screener/stocks/movers?top=10"),
        ("most actives", f"{DATA}/v1beta1/screener/stocks/most-actives?top=10"),
    ):
        try:
            r = await client.get(url, headers=AUTH, timeout=15)
        except Exception as exc:  # noqa: BLE001
            print(f"\n== {name}: {bot.describe_error(exc)}")
            continue
        print(f"\n== {name}: HTTP {r.status_code} ({len(r.content)} B)")
        if r.status_code != 200:
            print("  ", r.text[:200])
            continue
        data = r.json()
        if name.startswith("news"):
            for n in data.get("news", [])[:10]:
                age = time.time() - iso_ts(n["created_at"])
                print(f"  {n['created_at']} ({age / 60:.0f} min ago) {n.get('source')} {n.get('symbols')[:4]} "
                      f"{n.get('headline', '')[:90]}")
        elif name.startswith("snapshots"):
            for sym, s in data.items():
                lt = (s or {}).get("latestTrade") or {}
                mb = (s or {}).get("minuteBar") or {}
                db = (s or {}).get("dailyBar") or {}
                pb = (s or {}).get("prevDailyBar") or {}
                print(f"  {sym}: last trade {lt.get('t')} ${lt.get('p')} · minute bar {mb.get('t')} · day vol "
                      f"{db.get('v')} · prev close {pb.get('c')}")
        else:
            print("  ", json.dumps(data)[:900])


async def race(client: httpx.AsyncClient) -> None:
    alpaca: dict[str, dict] = {}
    wires: dict[str, dict[str, float]] = {n: {} for n in WIRES}
    baseline: dict[str, set[str]] = {}
    end = time.time() + DURATION

    async def stream() -> None:
        try:
            async with websockets.connect(STREAM, open_timeout=15) as ws:
                print("stream:", await ws.recv())
                await ws.send(json.dumps({"action": "auth", "key": KEY, "secret": SECRET}))
                print("auth:", await ws.recv())
                await ws.send(json.dumps({"action": "subscribe", "news": ["*"]}))
                print("subscribe:", await ws.recv())
                while time.time() < end:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=max(1.0, end - time.time()))
                    except asyncio.TimeoutError:
                        break
                    now = time.time()
                    for m in json.loads(raw):
                        if m.get("T") != "n":
                            continue
                        alpaca.setdefault(norm(m.get("headline", "")), {
                            "recv": now, "created": iso_ts(m["created_at"]), "source": m.get("source", ""),
                            "symbols": m.get("symbols", []), "headline": m.get("headline", "")})
        except Exception as exc:  # noqa: BLE001
            print("stream failed:", bot.describe_error(exc))

    async def poll_wires() -> None:
        h = {"User-Agent": bot.WIRE_USER_AGENT, **bot.WIRE_HEADERS}
        while time.time() < end:
            started = time.time()
            for name, url in WIRES.items():
                try:
                    r = await client.get(url, headers=h, timeout=8)
                    titles = ([i.title for i in bot.parse_prn_list(r.text)] if name == "PRN page"
                              else [e.get("title", "") for e in feedparser.parse(r.content).entries])
                except Exception:  # noqa: BLE001
                    continue
                keys = {norm(t) for t in titles if t}
                if name not in baseline:
                    baseline[name] = keys
                    continue
                for k in keys - baseline[name]:
                    wires[name].setdefault(k, time.time())
            await asyncio.sleep(max(0.0, 3 - (time.time() - started)))

    await asyncio.gather(stream(), poll_wires())

    print(f"\n== race over {DURATION:.0f}s: Alpaca stream {len(alpaca)} items; "
          + ", ".join(f"{n} {len(v)} new" for n, v in wires.items()))
    lags = sorted(a["recv"] - a["created"] for a in alpaca.values())
    if lags:
        print(f"Alpaca delivery vs its own created_at: median {statistics.median(lags):.1f}s, max {lags[-1]:.1f}s")
    sources: dict[str, int] = {}
    for a in alpaca.values():
        sources[a["source"]] = sources.get(a["source"], 0) + 1
    print("Alpaca items by source:", sources)
    for name, seen in wires.items():
        both = [k for k in seen if k in alpaca]
        if not both:
            print(f"{name}: no shared headlines")
            continue
        d = sorted(seen[k] - alpaca[k]["recv"] for k in both)
        print(f"{name}: {len(both)} shared headlines; the wire saw them after Alpaca by median "
              f"{statistics.median(d):+.0f}s (min {d[0]:+.0f}s, max {d[-1]:+.0f}s)")
        for k in both[:8]:
            print(f"   {et(alpaca[k]['recv'])} alpaca vs {et(seen[k])} {name}: {alpaca[k]['headline'][:70]}")
    ours = set().union(*[set(v) for v in wires.values()]) if wires else set()
    extra = [a for k, a in alpaca.items() if k not in ours and a["symbols"]]
    print(f"\nAlpaca items with tickers that none of the polled wires listed: {len(extra)}")
    for a in extra[:25]:
        print(f"   {et(a['recv'])} ({a['recv'] - a['created']:.0f}s) {a['source']} {a['symbols'][:3]} {a['headline'][:80]}")


async def main() -> None:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        await rest_checks(client)
        if KEY and SECRET:
            await race(client)


if __name__ == "__main__":
    asyncio.run(main())
