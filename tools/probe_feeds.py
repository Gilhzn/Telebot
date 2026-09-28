"""Probe candidate news sources from the machine this runs on (used by the workflow's diag mode).

For index pages: print the RSS/feed links found on them.
For feed URLs: print HTTP status, number of items, first title and how many items carry a
US exchange ticker - i.e. whether the feed is usable by the bot.
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

INDEX_PAGES = [
    "https://www.businesswire.com/portal/site/home/news/rss/",
    "https://www.businesswire.com/newsroom/rss",
    "https://www.globenewswire.com/rss/list",
    "https://www.accessnewswire.com/newsroom/rss",
    "https://www.accessnewswire.com/rss-feeds",
    "https://www.newsfilecorp.com/rss",
]
FEEDS = [
    "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtRWA==",
    "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRWQ==",
    "https://www.accessnewswire.com/newsroom/api/rss",
    "https://www.accessnewswire.com/rss",
    "https://www.accesswire.com/rss/newsroom",
    "https://www.newsfilecorp.com/rss/latest",
    "https://feeds.newsfilecorp.com/latest",
    "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies",
    "https://www.globenewswire.com/RssFeed/subjectcode/12-Earnings%20Releases%20And%20Operating%20Results/feedTitle/GlobeNewswire%20-%20Earnings%20Releases%20And%20Operating%20Results",
    "https://www.globenewswire.com/RssFeed/industry/4000-Health%20Care/feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Health%20Care",
    "https://www.globenewswire.com/RssFeed/industry/9576-Semiconductors/feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Semiconductors",
] + sys.argv[1:]

HEADERS = {"User-Agent": bot.WIRE_USER_AGENT, **bot.WIRE_HEADERS}
LINK_RE = re.compile(r'href="([^"]*(?:rss|RssFeed|/feed)[^"]*)"', re.I)


def main() -> None:
    with httpx.Client(timeout=20, follow_redirects=True, headers=HEADERS) as client:
        print("=== index pages ===")
        for url in INDEX_PAGES:
            try:
                r = client.get(url)
                links = sorted({urljoin(str(r.url), h) for h in LINK_RE.findall(r.text)})
                print(f"{r.status_code} {url} -> {len(links)} feed links")
                for link in links[:60]:
                    print("    ", link)
            except Exception as exc:  # noqa: BLE001
                print(f"ERR {url}: {bot.describe_error(exc)}")
            time.sleep(1)
        print("=== feeds ===")
        for url in FEEDS:
            try:
                r = client.get(url)
                items = bot.parse_wire_feed(r.content) if r.status_code == 200 else []
                with_ticker = sum(1 for it in items if it.tickers)
                first = items[0].title[:80] if items else ""
                print(f"{r.status_code} items={len(items)} with_ticker={with_ticker} {url}\n     first: {first}")
            except Exception as exc:  # noqa: BLE001
                print(f"ERR {url}: {bot.describe_error(exc)}")
            time.sleep(1)


if __name__ == "__main__":
    main()
