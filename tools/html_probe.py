"""Print the markup around the first releases on the wires' newsroom pages (structure only, a
few KB, never stored) so the bot can parse them."""
from __future__ import annotations

import re
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

PAGES = {
    "https://www.prnewswire.com/news-releases/news-releases-list/?page=1&pagesize=100":
        r"/news-releases/[^\"'\s<>]*?-\d{9}\.html",
    "https://www.globenewswire.com/newsroom": r"/news-release/\d{4}/\d{2}/\d{2}/\d{6,}/",
}
H = {"User-Agent": bot.WIRE_USER_AGENT, **bot.WIRE_HEADERS}
for url, pat in PAGES.items():
    r = httpx.get(url, headers=H, timeout=20, follow_redirects=True)
    body = r.text
    print(f"\n===== {r.status_code} {len(body)}B {url}")
    hits = [m.start() for m in re.finditer(pat, body)]
    print("release links:", len(hits))
    if not hits:
        i = body.find("news-release")
        print("no match; around 'news-release':", re.sub(r"\s+", " ", body[max(0, i - 500):i + 1500]))
        print("hrefs sample:", re.findall(r'href="([^"]+)"', body)[40:80])
        continue
    for h in hits[:2]:
        print("-----")
        print(re.sub(r"\s+", " ", body[max(0, h - 1200):h + 1500]))
