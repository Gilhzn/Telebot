"""Which public channel shows a new press release first?

Polls several channels every INTERVAL seconds for DURATION seconds and records, per release
id, when each channel first listed it. Releases already listed at start are ignored. Prints
per-release lead times (e.g. HTML newsroom vs RSS) and the RSS lag behind the release's own
pubDate. Run from GitHub Actions: .github/workflows/probe.yml.
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

DURATION = float(sys.argv[1]) if len(sys.argv) > 1 else 170.0
INTERVAL = 10.0

PRN_ID = re.compile(r"/news-releases/[^\"'\s<>]*?-(\d{9})\.html")
GLOBE_ID = re.compile(r"/news-release/\d{4}/\d{2}/\d{2}/(\d{6,})/")
# channel name -> (wire, kind, url)
CHANNELS = {
    "PRN rss (all)": ("prn", "rss", "https://www.prnewswire.com/rss/news-releases-list.rss"),
    "PRN html list": ("prn", "html", "https://www.prnewswire.com/news-releases/news-releases-list/"),
    "PRN html list p100": ("prn", "html",
                           "https://www.prnewswire.com/news-releases/news-releases-list/?page=1&pagesize=100"),
    "Globe rss (public cos)": ("globe", "rss", next(u for u in bot._split(bot.DEFAULT_WIRE_FEEDS) if "orgclass" in u)),
    "Globe html newsroom": ("globe", "html", "https://www.globenewswire.com/newsroom"),
}
ID_RE = {"prn": PRN_ID, "globe": GLOBE_ID}
HEADERS = {"User-Agent": bot.WIRE_USER_AGENT, **bot.WIRE_HEADERS}


def ids_in(wire: str, kind: str, body: bytes) -> dict[str, float | None]:
    """release id -> pubDate (epoch) where known."""
    if kind == "rss":
        out: dict[str, float | None] = {}
        for e in feedparser.parse(body).entries:
            m = ID_RE[wire].search(e.get("link", "") or e.get("id", ""))
            if m:
                ts = e.get("published_parsed") or e.get("updated_parsed")
                out[m.group(1)] = float(calendar.timegm(ts)) if ts else None
        return out
    return {m.group(1): None for m in ID_RE[wire].finditer(body.decode("utf-8", "replace"))}


async def main() -> None:
    first_seen: dict[str, dict[str, float]] = {name: {} for name in CHANNELS}
    baseline: dict[str, set[str]] = {}
    pubdate: dict[str, float] = {}
    errors: dict[str, int] = {name: 0 for name in CHANNELS}
    async with httpx.AsyncClient(timeout=10, follow_redirects=True, headers=HEADERS) as client:
        async def poll(name: str) -> None:
            wire, kind, url = CHANNELS[name]
            try:
                r = await client.get(url)
                r.raise_for_status()
            except Exception:  # noqa: BLE001
                errors[name] += 1
                return
            now = time.time()
            found = ids_in(wire, kind, r.content)
            if name not in baseline:
                baseline[name] = set(found)
                print(f"{name}: {len(found)} releases listed at start")
                return
            for rid, ts in found.items():
                first_seen[name].setdefault(rid, now)
                if ts:
                    pubdate.setdefault(rid, ts)

        end = time.time() + DURATION
        while time.time() < end:
            started = time.time()
            await asyncio.gather(*(poll(n) for n in CHANNELS))
            await asyncio.sleep(max(0.0, INTERVAL - (time.time() - started)))

    old = set().union(*baseline.values()) if baseline else set()
    print(f"\npolled every {INTERVAL:.0f}s for {DURATION:.0f}s; errors per channel: {errors}")
    for wire in ("prn", "globe"):
        names = [n for n, (w, _, _) in CHANNELS.items() if w == wire]
        new_ids = sorted({rid for n in names for rid in first_seen[n]} - old)
        print(f"\n== {wire}: {len(new_ids)} new releases ==")
        leads: dict[str, list[float]] = {n: [] for n in names}
        for rid in new_ids:
            seen = {n: first_seen[n][rid] for n in names if rid in first_seen[n]}
            earliest = min(seen.values())
            row = "  ".join(f"{n}=+{seen[n] - earliest:.0f}s" if n in seen else f"{n}=never" for n in names)
            lag = f"  rss lag vs pubDate={min(v for k, v in seen.items() if 'rss' in k) - pubdate[rid]:.0f}s" \
                if rid in pubdate and any("rss" in k for k in seen) else ""
            print(f"  {rid}: {row}{lag}")
            for n in names:
                if n in seen:
                    leads[n].append(seen[n] - earliest)
        for n, vals in leads.items():
            if vals:
                print(f"  {n}: saw {len(vals)}/{len(new_ids)}, behind the first channel by "
                      f"median {statistics.median(vals):.0f}s, max {max(vals):.0f}s")


if __name__ == "__main__":
    asyncio.run(main())
