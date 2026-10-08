"""Source race: which public channel shows a new market-moving item first, and how far behind
the item's own timestamp each channel is.

Polls every channel every INTERVAL seconds for DURATION seconds. Items already listed at the
start are ignored. For each wire it compares its RSS feed with its HTML newsroom (same release
ids), and for every feed it measures the lag behind the release's own publish time. It also
lists new Nasdaq trading halts (T1 = news pending, LUDP = limit-up volatility pause), the
exchange's own first-hand signal. Run from GitHub Actions: .github/workflows/probe.yml.
"""
from __future__ import annotations

import asyncio
import calendar
import re
import statistics
import sys
import time
from pathlib import Path

import feedparser
import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

DURATION = float(sys.argv[1]) if len(sys.argv) > 1 else 1200.0
INTERVAL = 3.0
SEC_UA = bot.Config.from_env().sec_user_agent or "StockNewsRadar research contact@example.com"

ID_RE = {
    "prn": re.compile(r"/news-releases/[^\"'\s<>]*?-(\d{9})\.html"),
    "globe": re.compile(r"/news-release/\d{4}/\d{2}/\d{2}/(\d{6,})/"),
    "bw": re.compile(r"/news/home/(\d{14,})/en"),
    "sec": re.compile(r"/Archives/edgar/data/\d+/(\d{18})/|accession-number=(\d{10}-\d{2}-\d{6})"),
}
# channel -> (wire, kind, url, interval multiplier)
CHANNELS = {
    "PRN rss": ("prn", "rss", "https://www.prnewswire.com/rss/news-releases-list.rss", 1),
    "PRN html list": ("prn", "html", "https://www.prnewswire.com/news-releases/news-releases-list/?page=1&pagesize=100", 1),
    "Globe rss": ("globe", "rss", next(u for u in bot._split(bot.DEFAULT_WIRE_FEEDS) if "orgclass" in u), 1),
    "Globe html newsroom": ("globe", "html", "https://www.globenewswire.com/newsroom", 1),
    "BW rss": ("bw", "rss", "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtRWA==", 1),
    "BW html newsroom": ("bw", "html", "https://www.businesswire.com/newsroom?language=en", 2),
    "SEC 8-K atom": ("sec", "rss", bot.EDGAR_FEED_URL.format(form="8-K"), 1),
}
HALTS_URL = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"


def headers_for(wire: str) -> dict[str, str]:
    return {"User-Agent": SEC_UA} if wire == "sec" else {"User-Agent": bot.WIRE_USER_AGENT, **bot.WIRE_HEADERS}


def ids_in(wire: str, kind: str, body: bytes) -> dict[str, float | None]:
    """release id -> its own publish time (epoch) where the channel gives one."""
    if kind == "rss":
        out: dict[str, float | None] = {}
        for e in feedparser.parse(body).entries:
            m = ID_RE[wire].search(e.get("link", "") or e.get("id", ""))
            if m:
                ts = e.get("published_parsed") or e.get("updated_parsed")
                out[next(g for g in m.groups() if g) if m.groups() else m.group(0)] = \
                    float(calendar.timegm(ts)) if ts else None
        return out
    return {next(g for g in m.groups() if g): None for m in ID_RE[wire].finditer(body.decode("utf-8", "replace"))}


async def main() -> None:
    first_seen: dict[str, dict[str, float]] = {n: {} for n in CHANNELS}
    baseline: dict[str, set[str]] = {}
    pubdate: dict[str, float] = {}
    errors: dict[str, list[str]] = {n: [] for n in [*CHANNELS, "halts"]}
    timings: dict[str, list[float]] = {n: [] for n in [*CHANNELS, "halts"]}
    halts_seen: set[str] = set()
    halts_new: list[str] = []
    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
        async def poll(name: str) -> None:
            wire, kind, url, _ = CHANNELS[name]
            t0 = time.time()
            try:
                r = await client.get(url, headers=headers_for(wire))
                r.raise_for_status()
            except Exception as exc:  # noqa: BLE001
                errors[name].append(bot.describe_error(exc)[:80])
                return
            timings[name].append(time.time() - t0)
            now = time.time()
            found = ids_in(wire, kind, r.content)
            if name not in baseline:
                baseline[name] = set(found)
                print(f"{name}: {len(found)} items listed at start ({len(r.content) // 1024} KB, {timings[name][-1]:.1f}s)")
                return
            for rid, ts in found.items():
                if rid not in baseline[name]:
                    first_seen[name].setdefault(rid, now)
                if ts:
                    pubdate.setdefault(f"{wire}:{rid}", ts)

        async def poll_halts(first: bool) -> None:
            t0 = time.time()
            try:
                r = await client.get(HALTS_URL, headers=bot.RESEARCH_HEADERS)
                r.raise_for_status()
            except Exception as exc:  # noqa: BLE001
                errors["halts"].append(bot.describe_error(exc)[:80])
                return
            timings["halts"].append(time.time() - t0)
            for e in feedparser.parse(r.content).entries:
                sym = e.get("ndaq_issuesymbol") or e.get("title", "")
                key = f"{sym}|{e.get('ndaq_haltdate', '')}|{e.get('ndaq_halttime', '')}|{e.get('ndaq_reasoncode', '')}"
                if key in halts_seen:
                    continue
                halts_seen.add(key)
                if not first:
                    et = bot.us_eastern_now().strftime("%H:%M:%S")
                    halts_new.append(f"  seen {et} ET: {sym} halt {e.get('ndaq_halttime', '?')} code "
                                     f"{e.get('ndaq_reasoncode', '?')} ({e.get('ndaq_issuename', '')[:40]})")
                    print(halts_new[-1])
            if first:
                print(f"halts: {len(halts_seen)} listed at start")

        end = time.time() + DURATION
        cycle = 0
        while time.time() < end:
            started = time.time()
            due = [n for n, c in CHANNELS.items() if cycle % c[3] == 0]
            await asyncio.gather(*(poll(n) for n in due), poll_halts(cycle == 0))
            cycle += 1
            await asyncio.sleep(max(0.0, INTERVAL - (time.time() - started)))

    print(f"\npolled every {INTERVAL:.0f}s for {DURATION:.0f}s")
    for n in [*CHANNELS, "halts"]:
        t = timings[n]
        print(f"  {n}: {len(t)} ok, {len(errors[n])} errors"
              + (f", response median {statistics.median(t):.2f}s" if t else "")
              + (f", e.g. {errors[n][0]}" if errors[n] else ""))
    for wire in ("prn", "globe", "bw", "sec"):
        names = [n for n, c in CHANNELS.items() if c[0] == wire]
        new_ids = sorted({rid for n in names for rid in first_seen[n]})
        print(f"\n== {wire}: {len(new_ids)} new items ==")
        leads: dict[str, list[float]] = {n: [] for n in names}
        lags: list[float] = []
        for rid in new_ids:
            seen = {n: first_seen[n][rid] for n in names if rid in first_seen[n]}
            earliest = min(seen.values())
            for n in names:
                if n in seen:
                    leads[n].append(seen[n] - earliest)
            if f"{wire}:{rid}" in pubdate:
                lags.append(earliest - pubdate[f"{wire}:{rid}"])
        for n, vals in leads.items():
            if vals:
                print(f"  {n}: saw {len(vals)}/{len(new_ids)}, behind the first channel median "
                      f"{statistics.median(vals):.0f}s, p90 {sorted(vals)[int(len(vals) * 0.9)]:.0f}s, max {max(vals):.0f}s")
        if lags:
            lags.sort()
            print(f"  first sighting vs the item's own timestamp: median {statistics.median(lags):.0f}s, "
                  f"p10 {lags[len(lags) // 10]:.0f}s, p90 {lags[int(len(lags) * 0.9)]:.0f}s")
    print(f"\n== Nasdaq halts: {len(halts_new)} new ==")


if __name__ == "__main__":
    asyncio.run(main())
