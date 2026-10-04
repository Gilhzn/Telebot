"""Probe gurufocus.com from a runner: page structure, robots.txt and terms on automated access.
Prints short extracts only (no raw pages are stored)."""
from __future__ import annotations

import re
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

UA = {"User-Agent": bot.WIRE_USER_AGENT, "Accept": "text/html,application/xhtml+xml"}


def text(markup: str) -> str:
    return re.sub(r"\s+", " ", bot.html_to_text(markup))


def main() -> None:
    with httpx.Client(timeout=25, follow_redirects=True, headers=UA) as c:
        r = c.get("https://www.gurufocus.com/robots.txt")
        print("robots.txt", r.status_code)
        for line in r.text.splitlines():
            if re.search(r"user-agent|disallow: /(guru|$)|crawl-delay|/guru", line, re.I):
                print("   ", line.strip())
        for url in ("https://www.gurufocus.com/term-of-use", "https://www.gurufocus.com/terms-of-use",
                    "https://www.gurufocus.com/term_of_use.php"):
            r = c.get(url)
            t = text(r.text)
            print("terms", url, r.status_code, len(t))
            for m in re.finditer(r"[^.]{0,300}(scrap|robot|spider|crawl|automat|data min)[^.]{0,300}\.", t, re.I):
                print("   >>", m.group(0).strip()[:600])
        r = c.get("https://www.gurufocus.com/guru/top-holdings")
        body = r.text
        print("\ntop-holdings", r.status_code, len(body), r.headers.get("server"), r.headers.get("cf-ray"))
        print("stock links:", len(re.findall(r'href="/stock/[A-Z.\-]+', body)),
              "nuxt state:", "__NUXT__" in body, "json-ld:", body.count("application/ld+json"))
        t = text(body)
        i = t.lower().find("top holdings")
        print("TEXT:", t[max(0, i - 200): i + 3000])
        for m in re.finditer(r"/(?:reader|_api|api)/[A-Za-z0-9_/\-?=&.]{4,80}", body):
            print("   api:", m.group(0))


if __name__ == "__main__":
    main()
