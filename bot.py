#!/usr/bin/env python3
"""Stock News Radar — personal Telegram bot for very positive US-stock news.

Listens to primary sources only (SEC EDGAR 8-K / 6-K live feeds and the
PR Newswire / GlobeNewswire RSS feeds), scores each item for positivity
(rules engine, or Claude when ANTHROPIC_API_KEY is set) and sends a Hebrew
alert to a single private Telegram chat when the score passes MIN_SCORE.

Usage:
    python bot.py            # run forever (server mode)
    python bot.py --run-for 3600  # poll continuously for an hour, then exit (GitHub Actions)
    python bot.py --once     # one pass over all sources, then exit
    python bot.py --test     # verify token, detect/save chat id, send a sample alert
"""
from __future__ import annotations

import argparse
import asyncio
import calendar
import datetime as dt
import html
import json
import logging
import os
import re
import signal
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urljoin, urlparse

import feedparser
import httpx
from dotenv import load_dotenv

log = logging.getLogger("radar")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EDGAR_FEED_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent"
    "&type={form}&count=100&output=atom"
)
# The exchange variant lets us keep only exchange-listed companies (no OTC).
TICKERS_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
# The PR Newswire "all news" feed holds only 20 items, which scroll out within minutes on a
# busy morning (that is how a KOD topline release was missed). Category feeds hold 20 items
# each for a narrower slice, so together they keep far more history between polls.
_PRN = "https://www.prnewswire.com/rss/"
DEFAULT_WIRE_FEEDS = ",".join([
    _PRN + "news-releases-list.rss",
    _PRN + "health-latest-news/health-latest-news-list.rss",
    _PRN + "health-latest-news/biotechnology-list.rss",
    _PRN + "health-latest-news/pharmaceuticals-list.rss",
    _PRN + "financial-services-latest-news/financial-services-latest-news-list.rss",
    _PRN + "technology-latest-news/technology-latest-news-list.rss",
    _PRN + "energy-latest-news/energy-latest-news-list.rss",
    _PRN + "heavy-industry-manufacturing-latest-news/heavy-industry-manufacturing-latest-news-list.rss",
    "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/"
    "GlobeNewswire%20-%20News%20about%20Public%20Companies",
    *(f"https://www.globenewswire.com/RssFeed/industry/{code}-{quote(name)}/feedTitle/"
      f"GlobeNewswire%20-%20Industry%20News%20on%20{quote(name)}"
      for code, name in (("4000", "Health Care"), ("4573", "Biotechnology"), ("4577", "Pharmaceuticals"),
                         ("9576", "Semiconductors"), ("2717", "Defense"), ("2713", "Aerospace"))),
    # Business Wire "All News". Its releases rarely carry the ticker in the feed and its site
    # blocks automated page reads, so tickers come from the company name in the headline.
    "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtRWA==",
])
NO_PAGE_HOSTS = ("businesswire.com",)
TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
DEFAULT_MODEL = "claude-haiku-4-5-20251001"

# An honest RSS-reader identity. Wire sites' bot protection drops requests that claim
# to be a browser without behaving like one (seen from GitHub Actions: timeouts / 503).
WIRE_USER_AGENT = "StockNewsRadar/1.0 (personal RSS reader; +https://github.com/Gilhzn/Telebot)"
WIRE_HEADERS = {
    "Accept": "application/rss+xml, application/xml;q=0.9, text/html;q=0.8, */*;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
}
WIRE_ATTEMPTS = 3                # retries on timeout / redirect / 5xx
TOKEN_RE = re.compile(r"^\d{5,}:[A-Za-z0-9_-]{30,}$")

FEED_TIMEOUT = 10.0              # seconds for a live feed poll (a stuck poll delays alerts)
SEC_BLOCK_SECONDS = 600          # wait 10 minutes after 403/429 from SEC
SEC_MIN_INTERVAL = 0.15          # <= ~7 requests/second, well under SEC's 10/s
TICKER_REFRESH_SECONDS = 24 * 3600
MAX_SEEN = 30_000
AI_CONCURRENCY = 5
ALERT_LOG_MAX = 5_000
PERF_REPORT_AT = (20, 10)        # New York time: daily performance report after the evening session
CATEGORY_FEED_EVERY = 6          # continuous mode: category wire feeds every 6th wire poll
AI_TEXT_CHARS = 6_000
SMALL_CAP_PRICE = 5.0            # learned: two thirds of the 20%+ gainers trade under $5
LEAD_CHARS = 2_500               # rules score the headline + lead only
NEGATIVE_CHARS = 4_000           # negative filter window
ALERT_AGE_LIMIT = 3600           # the ⏱ line is shown only for items younger than 1h

# Hebrew names of 8-K items.
ITEMS_HE = {
    "1.01": "חתימה על הסכם מהותי",
    "1.02": "סיום הסכם מהותי",
    "1.03": "פשיטת רגל או כינוס נכסים",
    "1.04": "בטיחות מכרות",
    "1.05": "אירוע סייבר מהותי",
    "2.01": "השלמת רכישה או מכירה של נכסים",
    "2.02": "תוצאות כספיות",
    "2.03": "יצירת התחייבות פיננסית",
    "2.04": "אירוע שמאיץ התחייבות",
    "2.05": "עלויות יציאה מפעילות",
    "2.06": "ירידת ערך מהותית",
    "3.01": "הודעה על מחיקה או אי-עמידה בכללי הרישום",
    "3.02": "מכירת מניות שלא נרשמה",
    "3.03": "שינוי מהותי בזכויות בעלי המניות",
    "4.01": "החלפת רואה החשבון",
    "4.02": "אי-הסתמכות על דוחות קודמים",
    "5.01": "שינוי בשליטה",
    "5.02": "שינויים בהנהלה ובדירקטוריון",
    "5.03": "שינוי בתקנון או בשנת הכספים",
    "5.04": "השעיית מסחר בתוכניות עובדים",
    "5.05": "שינוי בקוד האתי",
    "5.06": "יציאה מסטטוס חברת מעטפת",
    "5.07": "הצבעות באסיפת בעלי המניות",
    "5.08": "מועמדויות לדירקטוריון",
    "6.01": "מידע על ניירות מגובי נכסים",
    "7.01": "גילוי לפי Regulation FD",
    "8.01": "אירועים אחרים",
    "9.01": "דוחות כספיים ונספחים",
}

# ---------------------------------------------------------------------------
# Scoring rules
# ---------------------------------------------------------------------------

I = re.IGNORECASE

# (category label in Hebrew, pattern). Any hit disqualifies the item.
NEGATIVE_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("הנפקה ודילול", re.compile(
        r"\b(?:public offering|registered direct offering|underwritten offering|"
        r"proposed offering|pricing of (?:its |an? |the )?(?:\$[\d.,]+ (?:million|billion) )?"
        r"(?:public |underwritten |registered |upsized |common stock |convertible )*offering|"
        r"prices? (?:its |an? |the )?(?:\$[\d.,]+ (?:million|billion) )?"
        r"(?:public |underwritten |registered |upsized |common stock )*offering|"
        r"equity offering|secondary offering|follow-on offering|"
        r"at[- ]the[- ]market (?:offering|program|equity)|\bATM (?:offering|program)|"
        r"private placement|securities purchase agreement|warrant inducement|"
        r"convertible (?:senior )?notes? offering|shelf registration|equity line of credit)\b", I)),
    ("איחוד מניות (reverse split)", re.compile(
        r"\breverse (?:stock |share )?split\b|\bshare consolidation\b", I)),
    ("אזהרת מחיקה / אי-עמידה", re.compile(
        r"\b(?:delisting (?:notice|determination|notification)|notice of delisting|"
        r"(?:notice|notification|letter) (?:of|regarding) (?:non-?compliance|deficiency)|"
        r"deficiency (?:letter|notice)|minimum bid price|not in compliance with|"
        r"non-?compliance with (?:nasdaq|nyse|listing))\b", I)),
    ("going concern / פשיטת רגל", re.compile(
        r"\b(?:going concern|chapter (?:7|11)|bankruptcy|insolvency|insolvent|"
        r"restructuring support agreement|forbearance agreement)\b", I)),
    ("תביעה / התראת משרד עורכי דין", re.compile(
        r"\b(?:class action|investor alert|shareholder alert|securities fraud|lawsuit|"
        r"lead plaintiff|securities litigation|law firm|investigation on behalf of|"
        r"investigating (?:potential |possible )?(?:claims|securities|breaches)|"
        r"rosen law|pomerantz|levi (?:&|and) korsinsky|bragar eagel|kessler topaz|"
        r"faruqi|glancy prongay|bronstein, gewirtz|schall law|robbins geller|"
        r"halper sadeh|kahn swick|johnson fistel|block & leviton)\b", I)),
    ("הצגה מחדש של דוחות", re.compile(
        r"\b(?:restatement of (?:its |the |our )?(?:previously issued |prior[- ]period )?financial|"
        r"restate (?:its |the |our )?(?:previously issued )?financial statements|"
        r"non-?reliance on previously issued|should no longer be relied upon)", I)),
    ("הורדת תחזית", re.compile(
        r"\b(?:(?:lowers|lowered|cuts|cut|reduces|reduced|withdraws|withdrew|suspends) "
        r"(?:its |the )?(?:full[- ]year |fiscal (?:year )?(?:\d{4} )?|\d{4} |annual |quarterly )?"
        r"(?:revenue |sales |earnings |financial )?(?:guidance|outlook|forecast))\b", I)),
    ("clinical hold / CRL", re.compile(
        r"\b(?:clinical hold|complete response letter|refusal to file|refuse to file|"
        r"did not meet (?:its |the )?primary endpoint|failed to meet (?:its |the )?primary endpoint)\b", I)),
    ("התפטרויות ופיטורים", re.compile(
        r"\b(?:resignation|resigns|resigned|to resign|steps? down|stepping down|layoffs?|"
        r"workforce reduction|reduction in (?:force|workforce)|reduce (?:its |our )?workforce|"
        r"lay off|laid off)\b", I)),
]

# (points, Hebrew label, pattern). Every hit adds points; total is capped at 5.
POSITIVE_RULES: list[tuple[int, str, re.Pattern[str]]] = [
    (5, "החברה נרכשת", re.compile(
        r"\b(?:to be acquired by|agreed to be acquired|agreement to be acquired|"
        r"definitive agreement to be acquired)\b", I)),
    (5, "אישור FDA", re.compile(
        r"\b(?:FDA approval|FDA approves|FDA has approved|FDA granted approval|"
        r"(?:Food and Drug Administration|FDA)(?: \(FDA\))? (?:has )?(?:approved|approves|granted approval))\b", I)),
    (4, "הצעה במזומן למניה", re.compile(r"\bper share in cash\b", I)),
    (4, "העלאת תחזית", re.compile(
        r"\b(?:raises|raised|increases|increased|boosts|lifts) (?:its |the )?"
        r"(?:full[- ]year |fiscal (?:year )?(?:\d{4} )?|\d{4} |annual )?"
        r"(?:revenue |sales |earnings |financial )?(?:guidance|outlook|forecast)\b", I)),
    (4, "עמידה ביעד הראשי בניסוי", re.compile(
        r"\b(?:met|meets|achieved|achieves) (?:its |the )?(?:primary|co-primary) endpoints?\b", I)),
    (4, "תוצאות topline חיוביות", re.compile(r"\bpositive (?:topline|top-line)\b", I)),
    (4, "הצטרפות למדד S&P 500", re.compile(
        r"\b(?:join|joins|joining|added to|to join|set to join)(?: the)? S&P 500\b", I)),
    (3, "זכייה בחוזה", re.compile(r"\bawarded\b", I)),
    (3, "זכייה בחוזה", re.compile(
        r"\b(?:wins|won|win|secures|secured) (?:an? )?(?:[\w$.,-]+ ){0,4}contracts?\b", I)),
    (3, "זכייה בחוזה", re.compile(r"\bcontract awards?\b", I)),
    (3, "השקעה אסטרטגית", re.compile(r"\bstrategic investment\b", I)),
    (3, "הכנסות שיא", re.compile(r"\brecord (?:quarterly |annual |full[- ]year )?revenues?\b", I)),
    (4, "Breakthrough Designation מה-FDA", re.compile(r"\bbreakthrough (?:therapy|device)(?: designation)?\b", I)),
    (4, "אישור FDA (clearance)", re.compile(
        r"\b(?:receives?|received|granted|obtains?|secures?)\b[^.\n]{0,30}\bFDA\b[^.\n]{0,25}\bclearance\b|"
        r"\bFDA (?:510\(k\) )?clearance\b", I)),
    # Learned from the gainers study: phrases behind small-cap jumps the bot used to score 0
    (3, "הזמנות / הזמנה חוזרת", re.compile(
        r"\b(?:secures?|secured|receives?|received|wins?|won|signs?|signed|books?|booked|lands?)\b"
        r"(?:[^.\n]|\.\d){0,45}\borders?\b|\brecord orders\b", I)),
    (3, "הסכם הפצה / רישוי בלעדי", re.compile(
        r"\bdefinitive (?:exclusive )?(?:distribution|license|licensing|supply|manufacturing|commercial)\b[^.\n]{0,40}"
        r"\bagreement\b|\bexclusive (?:distribution|license|licensing|supply) agreement\b", I)),
    (3, "זינוק ברווח / בהכנסות", re.compile(
        r"\b(?:profit|revenue|net income|earnings|sales)s?\b[^.\n]{0,15}\b(?:surged?|soared|jumped|grew|rose|increased)"
        r"\b[^.\n]{0,12}(?:\d+(?:\.\d+)? ?(?:times|x)\b|\d{3,}%)", I)),
    (3, "מיזוג", re.compile(
        r"\b(?:announce[sd]? (?:a |their |its )?(?:proposed |potential )?merger|merger agreement|agree(?:s|d)? to merge|"
        r"business combination|letter of intent to (?:combine|merge))\b", I)),
    (2, "שותפות", re.compile(r"\b(?:partners? with|partnership with|collaboration with|teams? up with)\b", I)),
    (2, "הסכם פריסה אצל לקוח", re.compile(r"\b(?:agreement for (?:the )?(?:phased )?deployment|deployment agreement)\b", I)),
    (2, "נבחרה על ידי לקוח", re.compile(r"\bselected by\b", I)),
    (2, "הזמנת רכש", re.compile(r"\bpurchase orders?\b", I)),
    (2, "הסכם רב-שנתי", re.compile(r"\bmulti-?year (?:agreement|contract|deal|supply agreement)\b", I)),
    (2, "שותפות אסטרטגית", re.compile(r"\bstrategic partnership\b", I)),
    (2, "רכישה עצמית של מניות", re.compile(
        r"\b(?:buyback|share repurchase|stock repurchase|repurchase program)\b", I)),
    (2, "לקוח ממשלתי", re.compile(
        r"(?i:\b(?:Department of Defense|Department of War|U\.S\. Army|Army|Navy|Air Force|"
        r"Space Force|Marine Corps|DARPA|Pentagon|Missile Defense Agency)\b)|\bDoD\b")),
    (1, "NASA", re.compile(r"\bNASA\b")),
]

# Overlapping phrases count once, at the higher score.
RULE_GROUPS = {"הזמנת רכש": "orders", "הזמנות / הזמנה חוזרת": "orders",
               "שותפות אסטרטגית": "partner", "שותפות": "partner"}

MEGA_COMPANIES = [
    "NVIDIA", "Nvidia", "Microsoft", "Amazon", "AWS", "Google", "Alphabet", "Apple",
    "Tesla", "OpenAI", "Meta", "Oracle", "Anthropic", "SpaceX", "IBM", "Intel", "AMD",
    "Broadcom", "Samsung", "TSMC", "Walmart", "Lockheed Martin", "Boeing", "Pfizer",
    "Eli Lilly", "Novartis", "Merck", "AstraZeneca", "Johnson & Johnson", "Salesforce",
    "Cisco", "Qualcomm", "Palantir", "xAI",
]
MEGA_RE = re.compile(r"\b(" + "|".join(re.escape(n) for n in MEGA_COMPANIES) + r")\b")
AMOUNT_RE = re.compile(
    r"(?:US)?\$\s?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*(billion|million|bn|mm|b|m)?\b", I)

# US exchange ticker pattern, e.g. "(NASDAQ: OKLO)", "NYSE American: UUUU", "Nasdaq:OKLO".
TICKER_RE = re.compile(
    r"(?i:\b(?:NASDAQ|NYSE|CBOE|BATS)(?:\s*(?:GS|GM|CM|Global\s+Select(?:\s+Market)?|"
    r"Global\s+Market|Capital\s+Market|American|MKT|Arca|BZX))?)"
    r"\s*:\s*([A-Z]{1,5}(?:[.\-][A-Z]{1,2})?)\b"
)

BOILERPLATE_RE = re.compile(
    r"(?im)^(?:about\s+[^\n]{1,80}$|"
    r"(?:cautionary\s+(?:note|statement)|forward[- ]looking\s+statements?|safe\s+harbor)[^\n]*$|"
    r"source:?\s+[^\n]{1,100}$|view original content[^\n]*$|"
    r"(?:media|investors?)\s+(?:relations\s+)?contacts?:?[^\n]*$|contacts?:?\s*$)"
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return default if value is None or not value.strip() else value.strip()


def _env_bool(name: str, default: bool) -> bool:
    value = _env(name)
    if not value:
        return default
    return value.lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except ValueError:
        return default


def _split(value: str) -> list[str]:
    return [p.strip() for p in value.split(",") if p.strip()]


@dataclass
class Config:
    telegram_token: str
    chat_id: str
    sec_user_agent: str
    anthropic_key: str = ""
    anthropic_model: str = DEFAULT_MODEL
    positive_only: bool = True
    min_score: int = 4
    marketwide: bool = True
    watchlist: list[str] = field(default_factory=list)
    candidate_items: set[str] = field(default_factory=lambda: {"1.01", "2.01", "2.02", "7.01", "8.01"})
    catalyst_alerts: bool = True
    pump_check: bool = True
    guru_alerts: bool = True
    jump_model: bool = True
    gurus: list[tuple[int, str, str]] = field(default_factory=lambda: list(DEFAULT_GURUS))
    edgar_forms: list[str] = field(default_factory=lambda: ["8-K", "6-K"])
    edgar_poll: float = 2.0
    wire_poll: float = 10.0
    wire_feeds: list[str] = field(default_factory=lambda: _split(DEFAULT_WIRE_FEEDS))
    wire_user_agent: str = WIRE_USER_AGENT
    dedup_hours: float = 6.0
    max_per_cycle: int = 15
    state_file: Path = Path("state.json")
    env_file: Path = Path(".env")

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            telegram_token=clean_token(_env("TELEGRAM_BOT_TOKEN")),
            chat_id=_env("TELEGRAM_CHAT_ID"),
            sec_user_agent=_env("SEC_USER_AGENT"),
            anthropic_key=_env("ANTHROPIC_API_KEY"),
            anthropic_model=_env("ANTHROPIC_MODEL", DEFAULT_MODEL),
            positive_only=_env_bool("POSITIVE_ONLY", True),
            min_score=max(-5, min(5, _env_int("MIN_SCORE", 4))),
            marketwide=_env_bool("MARKETWIDE", True),
            watchlist=[normalize_ticker(t) for t in _split(_env("WATCHLIST"))],
            candidate_items=set(_split(_env("CANDIDATE_ITEMS", "1.01,2.01,2.02,7.01,8.01"))),
            catalyst_alerts=_env_bool("CATALYST_ALERTS", True),
            pump_check=_env_bool("PUMP_CHECK", True),
            guru_alerts=_env_bool("GURU_ALERTS", True),
            jump_model=_env_bool("JUMP_MODEL", True),
            gurus=parse_gurus(_env("GURUS")) or list(DEFAULT_GURUS),
            edgar_forms=[f.upper() for f in _split(_env("EDGAR_FORMS", "8-K,6-K"))],
            edgar_poll=max(1.0, _env_float("EDGAR_POLL_SECONDS", 2.0)),
            wire_poll=max(2.0, _env_float("WIRE_POLL_SECONDS", 10.0)),
            wire_feeds=_split(_env("WIRE_FEEDS", DEFAULT_WIRE_FEEDS)),
            wire_user_agent=_env("WIRE_USER_AGENT", WIRE_USER_AGENT),
            dedup_hours=_env_float("DEDUP_HOURS", 6.0),
            max_per_cycle=max(1, _env_int("MAX_ALERTS_PER_CYCLE", 15)),
            state_file=Path(_env("STATE_FILE", "state.json")),
            env_file=Path(_env("ENV_FILE", ".env")),
        )


def clean_token(raw: str) -> str:
    """Forgive common copy/paste mistakes: quotes, whitespace, a leading 'bot'."""
    token = re.sub(r"\s+", "", raw.strip().strip("'\""))
    if re.match(r"(?i)^bot\d+:", token):
        token = token[3:]
    return token


def token_hint(token: str) -> str:
    """Describe the token's shape without revealing it."""
    if TOKEN_RE.match(token):
        return "הפורמט תקין, אבל טלגרם לא מכיר את הטוקן — כנראה בוטל (/revoke) או הועתק מבוט אחר."
    if ":" not in token and 30 <= len(token) <= 40:
        return (f"נראה שנשמר רק החלק שאחרי הנקודתיים (אורך {len(token)}). "
                "חסר המספר שבתחילת הטוקן — יש להעתיק את כל הטוקן, כולל 123456789: בהתחלה.")
    return (f"הטוקן לא בפורמט הנכון (אורך {len(token)}, "
            f"{'יש' if ':' in token else 'אין'} נקודתיים). "
            "טוקן תקין נראה כך: 123456789:AAH... — מספר, נקודתיים ואז כ-35 תווים.")


def save_env_var(path: Path, key: str, value: str) -> None:
    """Set KEY=value in a .env file (create or replace the line), atomically."""
    lines: list[str] = []
    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()
    pattern = re.compile(rf"^\s*(?:export\s+)?{re.escape(key)}\s*=")
    for i, line in enumerate(lines):
        if pattern.match(line):
            lines[i] = f"{key}={value}"
            break
    else:
        lines.append(f"{key}={value}")
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    os.environ[key] = value


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

_BLOCK_TAGS = re.compile(
    r"</?(?:p|div|br|tr|li|ul|ol|h[1-6]|table|section|article|header|footer|blockquote|pre)\b[^>]*>", I)


def html_to_text(markup: str) -> str:
    s = re.sub(r"(?is)<(script|style|noscript|head|svg)\b.*?</\1\s*>", " ", markup)
    s = re.sub(r"(?s)<!--.*?-->", " ", s)
    s = _BLOCK_TAGS.sub("\n", s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s).replace("\xa0", " ").replace("\u200b", "")
    lines = (re.sub(r"[ \t\r\f\v]+", " ", ln).strip() for ln in s.split("\n"))
    return "\n".join(ln for ln in lines if ln)


def strip_boilerplate(text: str, min_pos: int = 200) -> str:
    """Cut the text at the first 'About X' / forward-looking / contacts section."""
    for m in BOILERPLATE_RE.finditer(text):
        if m.start() >= min_pos:
            return text[: m.start()].rstrip()
    return text


def skip_cover_page(text: str) -> str:
    """For a raw 8-K/6-K document, start at the first 'Item x.xx' heading."""
    m = re.search(r"(?m)^Item\s+\d\.\d\d\b", text)
    return text[m.start():] if m else text


ARTICLE_MARKERS = [
    r'itemprop="articleBody"',
    r'class="[^"]*\brelease-body\b',
    r'id="main-body-container"',
    r'class="[^"]*\barticle-body\b',
    r"<article\b",
]


def extract_article_text(page: str) -> str:
    """Body text of a press-release page, or '' when no known container is found."""
    for marker in ARTICLE_MARKERS:
        m = re.search(marker, page, I)
        if m:
            start = page.rfind("<", 0, m.start())
            return html_to_text(page[max(0, start): start + 80_000])[:20_000]
    return ""


def normalize_ticker(ticker: str) -> str:
    return ticker.strip().lstrip("$").upper().replace(".", "-")


def extract_tickers(text: str) -> list[str]:
    seen: list[str] = []
    for m in TICKER_RE.finditer(text):
        t = normalize_ticker(m.group(1))
        if t not in seen:
            seen.append(t)
    return seen


def esc(text: str) -> str:
    return html.escape(text or "", quote=False)


def fmt_duration(seconds: float) -> str:
    s = max(0, int(seconds))
    if s < 60:
        return "שנייה אחת" if s == 1 else f"{s} שניות"
    if s < 3600:
        m = s // 60
        return "דקה אחת" if m == 1 else f"{m} דקות"
    if s < 86400:
        h, m = divmod(s // 60, 60)
        hours = "שעה אחת" if h == 1 else f"{h} שעות"
        return hours if m == 0 else f"{hours} ו-{m} דקות"
    d, rem = divmod(s, 86400)
    days = "יום אחד" if d == 1 else f"{d} ימים"
    h = rem // 3600
    return days if h == 0 else f"{days} ו-{h} שעות"


def _struct_ts(value: Any) -> float | None:
    if not value:
        return None
    try:
        return float(calendar.timegm(value))
    except (TypeError, ValueError, OverflowError):
        return None


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


@dataclass
class RuleResult:
    score: int
    labels: list[str]
    reason_he: str


def negative_hit(text: str) -> str | None:
    for label, pattern in NEGATIVE_RULES:
        if pattern.search(text):
            return label
    return None


def _amount_usd(number: str, unit: str | None) -> float:
    value = float(number.replace(",", ""))
    unit = (unit or "").lower()
    if unit in ("billion", "bn", "b"):
        return value * 1e9
    if unit in ("million", "mm", "m"):
        return value * 1e6
    return value


STRONG_CHARS = 400  # strong phrases (4-5 points) must be in the headline or the opening
PERSONNEL_TITLE_RE = re.compile(
    r"\b(?:appoints?|appointment|names?|named|welcomes|hires|promotes|elects?)\b.{0,80}?"
    r"\b(?:chief|CEO|CFO|COO|CTO|president|director|officer|chair(?:man|woman)?|board)\b", I)


def rule_score(text: str, company: str = "", strong_text: str | None = None,
               title: str = "") -> RuleResult:
    """Rules score. 4-5 point phrases only count inside `strong_text` (headline + opening),
    and only inside the headline for personnel announcements, where a line like
    "...following FDA approval of Genio..." is company background, not news."""
    score = 0
    labels: list[str] = []
    if strong_text is None:
        strong_text = text
    if title and PERSONNEL_TITLE_RE.search(title):
        strong_text = title
    grouped: dict[str, int] = {}
    for points, label, pattern in POSITIVE_RULES:
        if pattern.search(strong_text if points >= 4 else text):
            group = RULE_GROUPS.get(label)
            if group:
                if points <= grouped.get(group, 0):
                    continue
                score -= grouped.get(group, 0)
                grouped[group] = points
            score += points
            if label not in labels:
                labels.append(label)
    target = re.search(r"\bto acquire\b(.{0,60})", strong_text, I)
    name_key = normalize_company(company).split(" ")[0] if company else ""
    if target and len(name_key) >= 3 and re.search(rf"\b{re.escape(name_key)}\b", target.group(1), I):
        score += 5
        labels.append("החברה נרכשת")
    if score > 0:
        company_l = company.lower()
        for m in MEGA_RE.finditer(text):
            name = m.group(1)
            if name.lower() not in company_l:
                score += 2
                labels.append(f"חברת ענק ({name})")
                break
        for m in AMOUNT_RE.finditer(text):
            if _amount_usd(m.group(1), m.group(2)) >= 100e6:
                score += 1
                labels.append("סכום של 100 מיליון דולר ומעלה")
                break
    score = min(score, 5)
    return RuleResult(score, labels, ", ".join(labels[:4]))


CLAUDE_SYSTEM = """You rate how positive a just-published news item is for the stock of the company it is about.
Return ONLY a JSON object, no other text: {"score": <int -5..5>, "ticker": "<ticker>", "reason_he": "<one short sentence in Hebrew>"}

Scale:
+5/+4 = very positive and material: the company is being acquired at a premium, FDA approval, a contract that is large relative to the company's size, a partnership with a giant customer, raised guidance.
+1..+3 = mildly positive or routine: small contract, product launch, award, conference appearance.
0 = neutral: board changes, meeting dates, credit facility.
negative = bad for the stock: offering/dilution, lawsuit, missed guidance, or the company is the ACQUIRER paying a rich price.

Be tough. Most news deserves 0 to 2. Give 4 or more only when a trader would expect a strong rise in the stock on this news.
reason_he must be in Hebrew, at most ~15 words, and explain the score."""


def parse_claude_json(text: str) -> dict[str, Any] | None:
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        return None
    try:
        data = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict) or "score" not in data:
        return None
    try:
        score = int(round(float(data["score"])))
    except (TypeError, ValueError):
        return None
    return {
        "score": max(-5, min(5, score)),
        "ticker": str(data.get("ticker") or ""),
        "reason_he": str(data.get("reason_he") or "")[:300],
    }


# ---------------------------------------------------------------------------
# Catalyst pre-alerts: "results will be published on <date>"
# ---------------------------------------------------------------------------

# (Hebrew label, pattern). Scheduled binary events that move a stock the moment they
# are published, so the user should know the date in advance.
CATALYST_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("תוצאות ניסוי קליני", re.compile(
        r"\b(?:to|will)\s+(?:present|announce|report|release|share|unveil|"
        r"host\b[^.\n]{0,80}?\bto discuss)\b[^.\n]{0,120}?"
        r"\b(?:topline|top-line|pivotal|phase\s*(?:3|iii|2b|2|ii)\b|primary endpoint)", I)),
    ("החלטת FDA (PDUFA)", re.compile(r"\bPDUFA\b[^.\n]{0,80}?\bdate\b", I)),
    ("ועדה מייעצת של ה-FDA", re.compile(
        r"\badvisory committee\b[^.\n]{0,80}?\b(?:meeting|scheduled|convene)", I)),
]
_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
HEBREW_MONTHS = ["ינואר", "פברואר", "מרץ", "אפריל", "מאי", "יוני", "יולי", "אוגוסט",
                 "ספטמבר", "אוקטובר", "נובמבר", "דצמבר"]
CATALYST_DATE_RE = re.compile(
    r"\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|Aug(?:ust)?|"
    r"Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+(\d{1,2})(?:st|nd|rd|th)?"
    r"(?:,?\s+(20\d\d))?\b", I)
RELATIVE_DAY_RE = re.compile(r"\b(today|tomorrow)\b", I)


@dataclass
class Catalyst:
    label: str
    date: dt.date


def us_eastern_now() -> dt.datetime:
    try:
        from zoneinfo import ZoneInfo
        return dt.datetime.now(ZoneInfo("America/New_York"))
    except Exception:  # noqa: BLE001  (no tz database: assume EDT)
        return dt.datetime.now(dt.timezone(dt.timedelta(hours=-4)))


def _catalyst_date(window: str, today: dt.date) -> dt.date | None:
    """First date in the text that follows the catalyst phrase (never the dateline before it)."""
    for m in CATALYST_DATE_RE.finditer(window):
        month = _MONTHS[m.group(1)[:3].lower()]
        year = int(m.group(3)) if m.group(3) else today.year
        try:
            day = dt.date(year, month, int(m.group(2)))
        except ValueError:
            continue
        if not m.group(3) and day < today - dt.timedelta(days=30):
            try:
                day = day.replace(year=year + 1)  # "January 5" written in December
            except ValueError:
                continue
        return day
    rel = RELATIVE_DAY_RE.search(window)
    if rel:
        return today if rel.group(1).lower() == "today" else today + dt.timedelta(days=1)
    return None


def find_catalyst(text: str, today: dt.date) -> Catalyst | None:
    """A scheduled catalyst with a concrete upcoming date, e.g.
    'Kodiak Sciences to Present Topline Results on September 28, 2026 from ... Phase 3 ...'."""
    # Abbreviations with periods ("8:00 a.m.", "U.S.", "Inc.") would end the phrase early.
    text = re.sub(r"\b([ap])\.m\.", r"\1m", text, flags=I)
    text = re.sub(r"\b(U)\.(S)\.", r"\1\2", text)
    text = re.sub(r"\b(Inc|Corp|Ltd|Co|No)\.", r"\1", text)
    for label, pattern in CATALYST_RULES:
        for m in pattern.finditer(text):
            day = _catalyst_date(text[m.start(): m.end() + 250], today)
            if day is not None and day >= today:
                return Catalyst(label, day)
    return None


def hebrew_date(day: dt.date) -> str:
    return f"{day.day} ב{HEBREW_MONTHS[day.month - 1]} {day.year}"


def days_until_he(day: dt.date, today: dt.date) -> str:
    n = (day - today).days
    return {0: "היום", 1: "מחר"}.get(n, f"בעוד {n} ימים")


# ---------------------------------------------------------------------------
# Pump risk: positive news that may spike and then crash
# ---------------------------------------------------------------------------

SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
DILUTION_FORMS = {"S-1", "S-1/A", "F-1", "F-1/A", "S-3", "S-3/A", "F-3", "F-3/A", "S-3ASR", "F-3ASR",
                  "424B1", "424B2", "424B3", "424B4", "424B5", "424B7"}
PUMP_TEXT_RULES: list[tuple[int, str, re.Pattern[str]]] = [
    (2, "הסכם לא מחייב (MOU / LOI)", re.compile(
        r"\b(?:non-?binding|memorandum of understanding|MOU|letter of intent|LOI|term sheet|"
        r"framework agreement|strategic cooperation agreement)\b", I)),
    (1, "סכום כותרת של \"עד\" (up to)", re.compile(r"\bup to (?:US)?\$\s?\d", I)),
    (1, "גל אופנתי (קריפטו / טרז'רי)", re.compile(
        r"\b(?:bitcoin|ethereum|solana|crypto(?:currency)?|digital asset treasury|treasury (?:strategy|reserve)|"
        r"token(?:ization)?|memecoin)\b", I)),
]


@dataclass
class PumpRisk:
    level: str            # "high" / "medium" / ""
    reasons: list[str]


def assess_pump_risk(text: str, submissions: dict[str, Any] | None, today: dt.date) -> PumpRisk:
    """Heuristic crash risk for a positive item: dilution capacity and exchange trouble in the
    company's recent SEC filings, plus pump-style wording in the news itself."""
    points, reasons = 0, []
    recent = ((submissions or {}).get("filings") or {}).get("recent") or {}
    forms, dates = recent.get("form") or [], recent.get("filingDate") or []
    items = recent.get("items") or [""] * len(forms)

    def within(days: int) -> list[tuple[str, dt.date, str]]:
        out = []
        for form, date_s, its in zip(forms, dates, items):
            try:
                d = dt.date.fromisoformat(date_s)
            except (TypeError, ValueError):
                continue
            if (today - d).days <= days:
                out.append((form, d, its or ""))
        return out

    year, half = within(365), within(182)
    # Large accelerated filers (public float of $700M+) file shelves and 424B notes for routine
    # debt financing; their offerings do not sink the stock, so only distress signals count.
    large = "large accelerated" in str((submissions or {}).get("category", "")).lower()
    shelf = [] if large else [f for f in year if f[0] in DILUTION_FORMS]
    if shelf:
        points += 2
        last = max(shelf, key=lambda f: f[1])
        reasons.append(f"מדף הנפקה / הנפקות ב-12 החודשים האחרונים ({last[0]}, {last[1]:%d.%m.%y}) — "
                       "החברה יכולה למכור מניות לתוך העלייה")
    if not large and sum(1 for f in half if f[0].startswith("424B")) >= 2:
        points += 1
        reasons.append("כמה תשקיפי מכירה (424B) בחצי השנה האחרונה")
    delist = [f for f in year if f[0].startswith("8-K") and "3.01" in f[2]]
    if delist:
        points += 2
        reasons.append(f"אזהרת מחיקה מהבורסה (8-K Item 3.01, {max(d for _, d, _ in delist):%d.%m.%y})")
    if any(f[0].startswith("8-K") and "3.02" in f[2] for f in year):
        points += 1
        reasons.append("מכירת מניות פרטית (8-K Item 3.02)")
    if any(f[0].startswith("8-K") and "3.03" in f[2] and "5.03" in f[2] for f in year):
        points += 1
        reasons.append("סימן לאיחוד מניות (reverse split) בשנה האחרונה")
    if any(f[0] in ("NT 10-K", "NT 10-Q", "NT 20-F") for f in year):
        points += 1
        reasons.append("איחור בהגשת דוחות כספיים (NT)")
    for pts, label, pattern in PUMP_TEXT_RULES:
        if pattern.search(text):
            points += pts
            reasons.append(label)
    level = "high" if points >= 4 else "medium" if points >= 2 else ""
    return PumpRisk(level, reasons if level else [])


# ---------------------------------------------------------------------------
# Guru portfolios: quarterly 13F filings of well-known investors (the data GuruFocus shows)
# ---------------------------------------------------------------------------

DEFAULT_GURUS: list[tuple[int, str, str]] = [
    (1067983, "Warren Buffett", "Berkshire Hathaway"),
    (1336528, "Bill Ackman", "Pershing Square"),
    (1649339, "Michael Burry", "Scion Asset Management"),
    (1061768, "Seth Klarman", "Baupost Group"),
    (1656456, "David Tepper", "Appaloosa"),
    (1536411, "Stanley Druckenmiller", "Duquesne Family Office"),
    (1709323, "Li Lu", "Himalaya Capital"),
    (1166559, "Bill Gates", "Gates Foundation Trust"),
    (949509, "Howard Marks", "Oaktree Capital"),
    (1350694, "Ray Dalio", "Bridgewater Associates"),
]
GURU_PAGE_URL = "https://www.gurufocus.com/guru/top-holdings"
GURU_CHECK_SECONDS = 1800
GURU_MIN_CHANGE = 0.10        # share count change that counts as "added" / "reduced"
GURU_LIST_MAX = 8             # lines per category in a message
SEC_FILING_INDEX_JSON = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nd}/index.json"
SEC_FILING_INDEX_HTML = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nd}/{acc}-index.htm"
OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"


def parse_gurus(raw: str) -> list[tuple[int, str, str]]:
    """GURUS="1067983:Warren Buffett:Berkshire Hathaway,1336528:Bill Ackman" """
    out = []
    for part in _split(raw):
        bits = [b.strip() for b in part.split(":")]
        if bits and bits[0].isdigit():
            out.append((int(bits[0]), bits[1] if len(bits) > 1 else bits[0], bits[2] if len(bits) > 2 else ""))
    return out


@dataclass
class Holding:
    cusip: str
    name: str
    value: float      # US dollars
    shares: float
    put_call: str = ""

    @property
    def key(self) -> str:
        return f"{self.cusip}:{self.put_call}" if self.put_call else self.cusip


def parse_13f_table(xml_text: str) -> dict[str, Holding]:
    """Information table of a 13F-HR filing -> holdings by CUSIP (+ put/call), lines merged."""
    root = ET.fromstring(xml_text.encode("utf-8") if isinstance(xml_text, str) else xml_text)
    out: dict[str, Holding] = {}
    for row in root.iter():
        if not row.tag.endswith("infoTable"):
            continue
        f = {el.tag.rsplit("}", 1)[-1]: (el.text or "").strip() for el in row.iter()}
        try:
            h = Holding(f.get("cusip", "").upper(), f.get("nameOfIssuer", ""), float(f.get("value") or 0),
                        float(f.get("sshPrnamt") or 0), f.get("putCall", "").title())
        except ValueError:
            continue
        if not h.cusip:
            continue
        if h.key in out:
            out[h.key].value += h.value
            out[h.key].shares += h.shares
        else:
            out[h.key] = h
    # Values are in dollars since 2023, but some filers still report thousands: then the implied
    # share price of a typical holding comes out below $1.
    prices = sorted(h.value / h.shares for h in out.values() if h.shares > 0)
    if prices and prices[len(prices) // 2] < 1.0:
        for h in out.values():
            h.value *= 1000
    return out


def diff_13f(prev: dict[str, Holding], cur: dict[str, Holding]) -> dict[str, list[tuple[Holding | None, Holding | None, float]]]:
    """new / added / reduced / sold, each (previous, current, share change), biggest moves first."""
    new = [(None, h, 1.0) for k, h in cur.items() if k not in prev]
    sold = [(h, None, -1.0) for k, h in prev.items() if k not in cur]
    added, reduced = [], []
    for k, h in cur.items():
        p = prev.get(k)
        if p is None or p.shares <= 0:
            continue
        change = h.shares / p.shares - 1
        if change >= GURU_MIN_CHANGE:
            added.append((p, h, change))
        elif change <= -GURU_MIN_CHANGE:
            reduced.append((p, h, change))

    def moved(t: tuple[Holding | None, Holding | None, float]) -> float:
        p, h, _ = t
        if h is None:
            return p.value if p else 0.0
        price = h.value / h.shares if h.shares else 0.0
        return abs(h.shares - (p.shares if p else 0.0)) * price

    return {k: sorted(v, key=moved, reverse=True)
            for k, v in (("new", new), ("added", added), ("reduced", reduced), ("sold", sold))}


def _money(x: float) -> str:
    for unit, div in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(x) >= div:
            return f"${x / div:,.1f}{unit}"
    return f"${x:,.0f}"


def quarter_label(report_date: str) -> str:
    try:
        d = dt.date.fromisoformat(report_date)
    except ValueError:
        return report_date
    return f"Q{(d.month - 1) // 3 + 1} {d.year}"


def format_guru_changes(guru: tuple[int, str, str], filing: dict[str, str], cur: dict[str, Holding],
                        changes: dict[str, list[tuple[Holding | None, Holding | None, float]]],
                        tickers: dict[str, str], first_report: bool) -> str:
    cik, name, fund = guru
    total = sum(h.value for h in cur.values())

    def label(h: Holding) -> str:
        t = tickers.get(h.cusip, "")
        opt = f" ({h.put_call})" if h.put_call else ""
        return (f"<b>{esc(t)}</b> {esc(h.name.title())}" if t else f"<b>{esc(h.name.title())}</b>") + opt

    filed = filing.get("filingDate", "")
    lines = [
        "🧭 <b>שינויים בתיק של משקיע-על</b>",
        f"<b>{esc(name)}</b>" + (f" · {esc(fund)}" if fund else ""),
        f"{quarter_label(filing.get('reportDate', ''))} · דווח ל-SEC ב-{esc(filed)} · "
        f"תיק {_money(total)} ב-{len(cur)} החזקות",
    ]
    if first_report:
        lines.append("ℹ️ זה הדיווח האחרון שכבר פורסם. מעכשיו תקבל הודעה על כל דיווח רבעוני חדש.")
    sections = [("new", "🟢 <b>קנו (פוזיציה חדשה)</b>"), ("added", "⬆️ <b>הגדילו</b>"),
                ("reduced", "⬇️ <b>הקטינו</b>"), ("sold", "🔴 <b>מכרו הכול</b>")]
    any_change = False
    for key, title in sections:
        rows = changes[key]
        if not rows:
            continue
        any_change = True
        lines.append(title)
        for p, h, change in rows[:GURU_LIST_MAX]:
            if key == "new":
                share = f" · {h.value / total * 100:.1f}% מהתיק" if total else ""
                lines.append(f"• {label(h)} · {_money(h.value)}{share}")
            elif key == "sold":
                lines.append(f"• {label(p)} · היה {_money(p.value)}")
            else:
                lines.append(f"• {label(h)} · {change * 100:+.0f}% מניות · עכשיו {_money(h.value)}")
        if len(rows) > GURU_LIST_MAX:
            lines.append(f"  ועוד {len(rows) - GURU_LIST_MAX}")
    if not any_change:
        lines.append("ללא שינויים מהותיים מהרבעון הקודם.")
    acc = filing.get("accessionNumber", "")
    sec_link = SEC_FILING_INDEX_HTML.format(cik=cik, acc_nd=acc.replace("-", ""), acc=acc)
    lines += [
        f'📰 מקור: <a href="{html.escape(GURU_PAGE_URL, quote=True)}">GuruFocus</a> · '
        f'הנתונים עצמם מהדיווח הרבעוני (13F) ל-SEC: <a href="{html.escape(sec_link, quote=True)}">הדיווח</a>',
        "ℹ️ דיווח 13F מתפרסם עד 45 יום אחרי סוף הרבעון, כך שהעסקאות עצמן נעשו קודם.",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Gainers study: why did today's biggest gainers rise, where did the news come from first,
# and did the bot see it in time? (data: Nasdaq market screener + 1-minute chart)
# ---------------------------------------------------------------------------

YAHOO_SPARK_URL = "https://query1.finance.yahoo.com/v8/finance/spark?symbols={symbols}&range={range}&interval=1d"
YAHOO_NEWS_URL = "https://query1.finance.yahoo.com/v1/finance/search?q={symbol}&newsCount={count}&quotesCount=0"
RESEARCH_HEADERS = {"User-Agent": "StockNewsRadar/1.0 (+https://github.com/Gilhzn/Telebot)",
                    "Accept": "application/json"}
SPARK_BATCH = 20
# Yahoo publisher names of the wires the bot reads itself
OUR_PUBLISHERS = ("globenewswire", "pr newswire", "prnewswire", "business wire", "businesswire")
GAINERS_REPORT_AT = (20, 20)      # New York time, after the performance report
GAINERS_MIN_PCT = 20.0            # a "big gainer": up 20%+ on the day
GAINERS_MIN_PRICE = 0.30
GAINERS_MIN_VOLUME = 300_000
GAINERS_TOP = 15
MOVE_START_PCT = 0.10             # the move starts at the first minute 10% above the previous close
NEWS_LOG_HOURS = 72
GAINERS_LOG_MAX = 4_000
LEARNING_READY_DAYS = 10          # trading days of data before the "ready" summary
SPLIT_ARTIFACT_PCT = 15.0         # a "gain" whose 1-minute bars never rose 15% is a reverse split, not a move
MOMENTUM_PCT = 15.0               # "breakout in progress": up 15%+ on the previous close
MOMENTUM_DOLLAR_VOLUME = 200_000  # with at least $200K traded today (no illiquid ticks)
MOMENTUM_NEWS_HOURS = 18          # watch tickers that had wire news in the last 18 hours
MOMENTUM_SCAN_SECONDS = 600       # full-market scan (regular session only: Yahoo spark has no pre-market)
MOMENTUM_MAX_CHECKS = 80          # chart requests per minute, at most

CATALYSTS: list[tuple[str, re.Pattern[str]]] = [
    ("FDA / רגולציה", re.compile(r"\b(?:FDA|EMA|clearance|cleared|breakthrough (?:therapy|device)|fast track|"
                                 r"orphan drug|PDUFA|approv\w+|510\(k\)|IND\b|marketing authori[sz]ation)", I)),
    ("תוצאות ניסוי קליני", re.compile(r"\b(?:topline|phase (?:1|2|3|i{1,3})\b|pivotal|primary endpoint|"
                                       r"clinical (?:data|results)|trial (?:met|results|data)|efficacy|remission)", I)),
    ("מיזוג / רכישה", re.compile(r"\b(?:to be acquired|acquire[sd]?|acquisition|merger|merge|buyout|"
                                 r"tender offer|take[- ]private|definitive agreement)", I)),
    ("חוזה / הזמנה", re.compile(r"\b(?:contract|award(?:ed)?|purchase order|orders?\b|selected by|wins?\b|"
                                r"task order|government|department of|army|navy|air force|pentagon|NASA)", I)),
    ("שותפות / רישיון", re.compile(r"\b(?:partner(?:ship)?|collaborat\w+|licens\w+|strategic (?:agreement|alliance)|"
                                   r"joint venture|MOU|memorandum|letter of intent|supply agreement|integrat\w+ with)", I)),
    ("קריפטו / AI / טרנד חם", re.compile(r"\b(?:bitcoin|crypto\w*|ethereum|solana|token\w*|treasury|blockchain|"
                                         r"artificial intelligence|\bAI\b|quantum|nuclear|uranium|drone|"
                                         r"robot\w*|data cent(?:er|re)|GPU|stablecoin)", I)),
    ("דוחות / תחזית", re.compile(r"\b(?:results|earnings|revenue|guidance|outlook|record (?:quarter|sales)|"
                                 r"profit|EPS|preliminary)", I)),
    ("החזר מניות / דיבידנד", re.compile(r"\b(?:buyback|repurchase|special dividend|dividend)", I)),
    ("הנפקה / איחוד מניות", re.compile(r"\b(?:offering|private placement|reverse (?:stock )?split|share consolidation|"
                                        r"registered direct|warrants?)", I)),
]
NO_NEWS = "ללא חדשות פומביות"


def classify_catalyst(title: str) -> str:
    for name, pattern in CATALYSTS:
        if pattern.search(title or ""):
            return name
    return "אחר" if title else NO_NEWS


def _pct_number(raw: Any) -> float | None:
    try:
        return float(str(raw).replace("%", "").replace("+", "").replace(",", "").replace("$", "").strip())
    except ValueError:
        return None


def research_universe(tickers: "TickerMap") -> list[str]:
    """Exchange-listed common shares: no warrants, units or rights (5-letter symbols ending W/U/R)."""
    return sorted(t for t in tickers.by_ticker
                  if re.fullmatch(r"[A-Z]{1,5}(?:-[A-Z])?", t) and not (len(t) == 5 and t[-1] in "WUR"))


def spark_moves(data: dict[str, Any], day: dt.date) -> dict[str, tuple[float, float, float]]:
    """symbol -> (% change, close, previous close) for `day` from a Yahoo spark response."""
    tz = eastern_tz()
    out = {}
    for sym, d in (data or {}).items():
        ts, closes = (d or {}).get("timestamp") or [], (d or {}).get("close") or []
        pairs = [(t, c) for t, c in zip(ts, closes) if c]
        if len(pairs) < 2 or dt.datetime.fromtimestamp(pairs[-1][0], tz).date() != day:
            continue
        prev, close = pairs[-2][1], pairs[-1][1]
        if prev > 0:
            out[normalize_ticker(sym)] = ((close / prev - 1) * 100, close, prev)
    return out


def yahoo_news_items(data: dict[str, Any], ticker: str) -> list[dict[str, Any]]:
    """News about `ticker` from a Yahoo search response: publish time, publisher, title."""
    out = []
    for n in (data or {}).get("news") or []:
        related = [normalize_ticker(t) for t in n.get("relatedTickers") or []]
        if related and ticker not in related:
            continue
        if n.get("providerPublishTime"):
            out.append({"pub": float(n["providerPublishTime"]), "src": str(n.get("publisher", "")),
                        "title": str(n.get("title", ""))[:140]})
    return out


def is_our_source(publisher: str) -> bool:
    return any(p in publisher.lower() for p in OUR_PUBLISHERS)


def move_profile(points: list[tuple[float, float]], prev_close: float | None) -> dict[str, Any]:
    """When the move started (first minute 10%+ above the previous close), the peak and when."""
    if not points or not prev_close:
        return {}
    start = next((t for t, px in points if px >= prev_close * (1 + MOVE_START_PCT)), None)
    peak_t, peak = max(points, key=lambda p: p[1])
    return {"start": start, "peak_t": peak_t, "peak_pct": (peak / prev_close - 1) * 100,
            "close_pct": (points[-1][1] / prev_close - 1) * 100}


def _median(values: list[float]) -> float | None:
    v = sorted(values)
    if not v:
        return None
    return v[len(v) // 2] if len(v) % 2 else (v[len(v) // 2 - 1] + v[len(v) // 2]) / 2


def learning_summary(log_entries: list[dict[str, Any]]) -> str:
    """What the gainers study has learned so far."""
    if not log_entries:
        return "📚 עדיין אין נתונים. הלמידה רצה בכל יום מסחר ב-20:20 שעון ניו יורק."
    days = sorted({e["date"] for e in log_entries})
    n = len(log_entries)
    with_ours = [e for e in log_entries if e.get("news_src") and e.get("seen")]
    elsewhere = [e for e in log_entries if e.get("news_src") and not e.get("seen")]
    none = [e for e in log_entries if not e.get("news_src")]
    lines = [f"📚 <b>מה למדתי עד עכשיו</b> ({len(days)} ימי מסחר, {n} מניות שעלו {GAINERS_MIN_PCT:.0f}%+)",
             f"• עם חדשות שהבוט ראה: {len(with_ours)} ({len(with_ours) * 100 // n}%)",
             f"• חדשות שהבוט לא ראה (מקור שלא במעקב או שפורסם קודם במקום אחר): {len(elsewhere)} "
             f"({len(elsewhere) * 100 // n}%)",
             f"• בלי חדשות פומביות (מומנטום, פמפום, סקטור): {len(none)} ({len(none) * 100 // n}%)"]
    lines.append("\n<b>איזה סוג חדשות מקפיץ מניות</b> (מספר · חציון עלייה ביום · חציון שיא)")
    cats: dict[str, list[dict[str, Any]]] = {}
    for e in log_entries:
        cats.setdefault(e.get("cat", NO_NEWS), []).append(e)
    for cat, rows in sorted(cats.items(), key=lambda kv: -len(kv[1])):
        med = _median([r["pct"] for r in rows])
        peak = _median([r["peak_pct"] for r in rows if r.get("peak_pct") is not None])
        lines.append(f"• {esc(cat)}: {len(rows)} · {med:+.0f}%" + (f" · שיא {peak:+.0f}%" if peak is not None else ""))
    srcs: dict[str, list[float]] = {}
    for e in with_ours + elsewhere:
        if e.get("lead_min") is not None:
            srcs.setdefault(e["news_src"].split(" · ")[0], []).append(e["lead_min"])
    if srcs:
        lines.append("\n<b>מאיפה החדשות הגיעו ראשונות</b> (מספר · חציון דקות מהפרסום עד תחילת הזינוק)")
        for src, leads in sorted(srcs.items(), key=lambda kv: -len(kv[1]))[:10]:
            mark = "" if is_our_source(src) or src.startswith(("SEC", "PR Newswire", "GlobeNewswire", "Business Wire")) \
                else " ⚠️ לא במעקב"
            lines.append(f"• {esc(src)}: {len(leads)} · {_median(leads):.0f} דק'{mark}")
    news_driven = [e for e in log_entries if e.get("news_src") and e.get("start")]
    small = [e for e in log_entries if 0 < e.get("price", 0) < 5]
    lines.append(f"\n• מניות מתחת ל-$5: {len(small) * 100 // n}% מהמזנקות")
    if news_driven:
        before = [e for e in news_driven if e.get("bot_lead_min") is not None and e["bot_lead_min"] >= 0]
        after = [e for e in news_driven if e.get("bot_lead_min") is not None and e["bot_lead_min"] < 0]
        missed = [e for e in news_driven if e.get("bot_lead_min") is None]
        lines.append(f"\n<b>הבוט מול זינוקים עם חדשות</b> ({len(news_driven)})")
        lines.append(f"• התריע לפני תחילת הזינוק: {len(before)}" +
                     (f" (חציון {_median([e['bot_lead_min'] for e in before]):.0f} דק' לפני)" if before else ""))
        lines.append(f"• התריע אחרי שהזינוק התחיל: {len(after)}")
        lines.append(f"• לא התריע (ציון נמוך או נפסל): {len(missed)}")
    sess: dict[str, int] = {}
    for e in log_entries:
        if e.get("start"):
            sess[session_of(e["start"])] = sess.get(session_of(e["start"]), 0) + 1
    if sess:
        names = {"pre": "טרום מסחר", "regular": "מסחר רגיל", "after": "אחרי המסחר", "closed": "סגור"}
        lines.append("\n<b>מתי הזינוקים מתחילים</b>: " +
                     " · ".join(f"{names[k]} {v}" for k, v in sorted(sess.items(), key=lambda kv: -kv[1])))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Jump model: how often news like this made the stock jump 20%+ in the past year.
# Learned by tools/catalysts.py from every 8-K/6-K press release and its price reaction;
# the features here are shared by the study and the live bot so both see the same thing.
# ---------------------------------------------------------------------------

JUMP_MODEL_FILE = Path(__file__).resolve().parent / "data" / "jump_model.json"
SEC_SHARES_URL = "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik:010d}/dei/EntityCommonStockSharesOutstanding.json"
YAHOO_DAILY_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=10d&interval=1d"
_MONTH_NAMES_RE = (r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|"
           r"Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)")
_DATE_RE = re.compile(r"\b" + _MONTH_NAMES_RE + r"\.?,?\s\d{1,2},?\s\d{4}|\((?:GLOBE NEWSWIRE|BUSINESS WIRE)\)|/PRNewswire|"
                      r"ACCESS Newswire|ACCESSWIRE|Newsfile Corp")
_CITY_RE = re.compile(r"(?:\s[A-Z][A-Z.'\-]+){1,3},?(?:\s[A-Z]{2,}[.,]?)*[\s,/–\-]*$|"
                      r"\s(?:(?:New|San|Los|Las|Fort|Salt|Palo|Santa|St\.|Saint|West|East|North|South|Boca|Kansas|"
                      r"Grand|Redwood|Menlo|Long|Woodland|Jersey|Oklahoma|Rancho|Newport|Bala)\s)?[A-Z][A-Za-z.'\-]+,"
                      r"\s(?:[A-Z]{2}|[A-Z][a-z]+\.?)[,:\s/–\-]*$|[\s,/–\-(]+$")
_PREFIX_RE = re.compile(r"^(?:EX-\d+[.\d]*\s+\d+\s+\S+\s+)?(?:EX-\d+[.\d]*\s+)?(?:Exhibit\s+\d+[.\d]*\s*)?"
                        r"(?:Press Release\s+|News Release\s+|For Immediate Release\s+)*", re.I)
_NOISE_RE = re.compile(r"\b(?:20\d\d|" + _MONTH_NAMES_RE.lower() + r"|exhibit|ex-\d\S*|htm|99|com|www|form|item|"
                       r"release|nasdaq|nyse)\b")
_STOP = set("the a an of and to for in on with by its at as from inc corp ltd llc co company announces announce "
            "announced reports report reported our has have will be is are that this".split())


def release_headline(lead: str) -> str:
    """The press release headline from the start of an exhibit ('' for a filing's own text)."""
    s = _PREFIX_RE.sub("", " ".join(lead.split()))
    if s.startswith(("Item ", "6-K ", "8-K ", "false ", "UNITED STATES")) or "Washington, D.C." in s[:200] \
            or re.match(r"\d{10} ", s):
        return ""
    m = _DATE_RE.search(s)
    if m:
        s = s[:m.start()]
    return _CITY_RE.sub("", s).strip()[:200]


def sector_of(sic: int | None) -> str:
    if not sic:
        return "לא ידוע"
    for lo, hi, name in ((2830, 2836, "ביוטק / פארמה"), (3840, 3851, "מכשור רפואי"), (8000, 8099, "שירותי בריאות"),
                         (7370, 7379, "תוכנה / IT"), (3570, 3579, "חומרה / שבבים"), (3670, 3679, "חומרה / שבבים"),
                         (3600, 3699, "אלקטרוניקה"), (3720, 3729, "תעופה / ביטחון"), (3760, 3769, "תעופה / ביטחון"),
                         (3710, 3716, "רכב"), (1300, 1399, "נפט וגז"), (1000, 1499, "כרייה"), (4900, 4999, "אנרגיה / תשתיות"),
                         (6770, 6770, "SPAC"), (6000, 6799, "פיננסים"), (4000, 4899, "תחבורה / תקשורת"),
                         (5000, 5999, "מסחר"), (2000, 3999, "תעשייה"), (8700, 8799, "שירותים / מחקר")):
        if lo <= sic <= hi:
            return name
    return "אחר"


def cap_bucket(mcap: float | None) -> str:
    if not mcap:
        return "שווי לא ידוע"
    return ("מתחת ל-$50M" if mcap < 50e6 else "$50M–300M" if mcap < 300e6 else "$300M–2B" if mcap < 2e9
            else "מעל $2B")


def price_bucket(px: float | None) -> str:
    if not px:
        return "מחיר לא ידוע"
    return "מתחת ל-$1" if px < 1 else "$1–5" if px < 5 else "$5–20" if px < 20 else "מעל $20"


def amount_bucket(text: str, mcap: float | None) -> str:
    amounts = [_amount_usd(m.group(1), m.group(2)) for m in AMOUNT_RE.finditer(text)]
    if not amounts or not mcap:
        return "ללא סכום"
    r = max(amounts) / mcap
    return ("סכום <5% משווי החברה" if r < 0.05 else "סכום 5–25% משווי החברה" if r < 0.25
            else "סכום 25–100% משווי החברה" if r < 1 else "סכום גדול משווי החברה")


def headline_phrases(text: str) -> set[str]:
    words = [w for w in re.findall(r"[a-z0-9$][a-z0-9$\-]*", text.lower()) if w not in _STOP and len(w) > 1]
    out = set(words)
    out |= {" ".join(words[i:i + 2]) for i in range(len(words) - 1)}
    out |= {" ".join(words[i:i + 3]) for i in range(len(words) - 2)}
    return {p for p in out if not re.fullmatch(r"[\d$.,\-]+", p) and not _NOISE_RE.search(p)}


def jump_features(r: dict[str, Any], vocab: set[str]) -> list[str]:
    """r: cat, cap, price, sector, session, pump, amount, score, pre5, headline, lead."""
    pre5 = r.get("pre5") or 0.0
    f = [f"cat={r['cat']}", f"cap={r['cap']}", f"price={r['price']}", f"sector={r['sector']}", f"sess={r['session']}",
         f"pump={r['pump']}", f"amount={r['amount']}", f"score={max(-1, min(5, r['score']))}",
         f"cat×cap={r['cat']}|{r['cap']}", f"pre5={'up' if pre5 > 0.2 else 'down' if pre5 < -0.2 else 'flat'}"]
    f += [f"p={p}" for p in headline_phrases(r["headline"] or r["lead"][:160]) if p in vocab]
    return f


def jump_probability(weights: dict[str, float], feats: list[str]) -> float:
    z = weights.get("bias", 0.0) + sum(weights.get(k, 0.0) for k in feats)
    return 1 / (1 + 2.718281828459045 ** -max(-30.0, min(30.0, z)))


def load_jump_model(path: Path = JUMP_MODEL_FILE) -> dict[str, Any] | None:
    try:
        model = json.loads(path.read_text(encoding="utf-8"))
        model["vocab_set"] = set(model.get("vocab", []))
        return model
    except (OSError, ValueError):
        return None


@dataclass
class JumpOdds:
    probability: float       # chance of a 20%+ jump within two days, from the past year
    base: float              # the same for an average press release
    reasons: list[str]       # the features that pushed it up most, in Hebrew

    @property
    def golden(self) -> bool:
        return self.probability >= 4 * self.base


FEATURE_HE = {"cat": "סוג החדשות", "cap": "שווי שוק", "price": "מחיר", "sector": "סקטור", "sess": "שעת פרסום",
              "pump": "סיכון פמפום", "amount": "גודל הסכום", "p": "בכותרת", "pre5": "מגמה בשבוע האחרון"}


def explain_features(weights: dict[str, float], feats: list[str], top: int = 3) -> list[str]:
    out = []
    for k in sorted(feats, key=lambda k: -weights.get(k, 0.0))[:top]:
        if weights.get(k, 0.0) < 0.25 or k.startswith(("score=", "cat×cap=")):
            continue
        name, _, value = k.partition("=")
        value = {"pre": "טרום מסחר", "regular": "מסחר רגיל", "after": "אחרי המסחר", "closed": "שוק סגור",
                 "up": "עלתה", "down": "ירדה", "flat": "יציבה"}.get(value, value)
        out.append(f"{FEATURE_HE.get(name, name)}: {value}" if name != "p" else f"„{value}” בכותרת")
    return out


# ---------------------------------------------------------------------------
# Trade outcome: "what if I had bought 3 minutes after the alert?"
# Used by the daily performance report and by tools/backtest.py.
# ---------------------------------------------------------------------------

Bar = tuple[float, float, float, float, float, float]   # start ts, open, high, low, close, volume
ENTRY_DELAY = 180                 # buy 3 minutes after the alert
MAX_ENTRY_GAP = 15 * 60           # no trade within 15 minutes of that -> market closed / no liquidity
TRADE_HORIZONS = [2, 5, 15, 30, 60, 120]                          # minutes after the entry
TRADE_TP_SL = [(0.02, 0.02), (0.05, 0.03), (0.10, 0.05), (0.20, 0.10)]
PERF_POSITION_USD = 2_000
PERF_COST = 0.01                  # round trip: spread + slippage, 0.5% per side
YAHOO_CHART_URL = ("https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?period1={p1}&period2={p2}"
                   "&interval={interval}m&includePrePost=true")


def quiet_stop(start: float, duration: float) -> float:
    """Shorten a timed run so it ends at a quiet minute (xx:14 or xx:44 New York): companies
    release news on the hour and half hour, and the next run needs about a minute to take over."""
    if duration < 40 * 60:
        return duration
    end = start + duration
    t = dt.datetime.fromtimestamp(end, eastern_tz()).replace(second=0, microsecond=0)
    back = (t.minute - 14) % 30
    candidate = t.timestamp() - back * 60
    return candidate - start if candidate - start >= duration / 2 else duration


def eastern_tz() -> dt.tzinfo:
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("America/New_York")
    except Exception:  # noqa: BLE001
        return dt.timezone(dt.timedelta(hours=-4))


def session_of(ts: float) -> str:
    t = dt.datetime.fromtimestamp(ts, eastern_tz())
    if t.weekday() >= 5:
        return "closed"
    minutes = t.hour * 60 + t.minute
    if 4 * 60 <= minutes < 9 * 60 + 30:
        return "pre"
    if 9 * 60 + 30 <= minutes < 16 * 60:
        return "regular"
    if 16 * 60 <= minutes < 20 * 60:
        return "after"
    return "closed"


def _close_at(bars: list[Bar], t: float, start_i: int) -> float:
    """Close of the last bar that started before t (bars from start_i on)."""
    px = bars[start_i][4]
    for b in bars[start_i:]:
        if b[0] >= t:
            break
        px = b[4]
    return px


def tp_sl(day_bars: list[Bar], entry: float, entry_ts: float, tp: float, sl: float) -> float:
    """Take profit / stop loss within the entry day; exit at the day's last bar otherwise.
    If a bar touches both levels, the stop counts (the conservative assumption)."""
    for b in day_bars:
        if b[0] == entry_ts:  # inside the entry minute only its close is known to be after the buy
            if b[4] <= entry * (1 - sl):
                return -sl
            if b[4] >= entry * (1 + tp):
                return tp
            continue
        if b[3] <= entry * (1 - sl):
            return -sl
        if b[2] >= entry * (1 + tp):
            return tp
    return day_bars[-1][4] / entry - 1 if day_bars else 0.0


def simulate_trade(bars: list[Bar], signal_ts: float) -> dict[str, Any] | None:
    """Buy at the open of the first bar starting at or after signal + 3:00 and measure the outcome."""
    if not bars:
        return None
    tz = eastern_tz()
    target = signal_ts + ENTRY_DELAY
    idx = next((i for i, b in enumerate(bars) if b[0] >= target - 1e-6), None)
    if idx is None:
        return None
    entry_ts, entry = bars[idx][0], bars[idx][1]
    if entry <= 0:
        return None
    gap = entry_ts - target
    before = [b for b in bars[:idx] if b[0] <= signal_ts]
    ref = before[-1][4] if before and signal_ts - before[-1][0] < 3 * 86400 else None
    out: dict[str, Any] = {
        "entry": entry, "entry_ts": entry_ts, "entry_delay_s": round(entry_ts - signal_ts),
        "entry_gap_s": round(gap), "session": session_of(signal_ts), "entry_session": session_of(entry_ts),
        "tradable": gap <= MAX_ENTRY_GAP, "ref": ref, "pre_move": (entry / ref - 1) if ref else None,
        "dollar_vol_5m": sum(b[4] * b[5] for b in bars[idx:idx + 5]),
    }
    for h in TRADE_HORIZONS:
        out[f"r_{h}m"] = _close_at(bars, entry_ts + h * 60, idx) / entry - 1
    day = lambda b: dt.datetime.fromtimestamp(b[0], tz).date()  # noqa: E731
    entry_day = day(bars[idx])
    day_bars = [b for b in bars[idx:] if day(b) == entry_day]
    regular = [b for b in day_bars if session_of(b[0]) == "regular"]
    out["r_close"] = (regular[-1][4] if regular else day_bars[-1][4]) / entry - 1
    later = [b for b in bars[idx:] if day(b) > entry_day]
    if later:
        nxt = [b for b in later if day(b) == day(later[0]) and session_of(b[0]) == "regular"]
        out["r_next_close"] = (nxt[-1][4] if nxt else later[-1][4]) / entry - 1
    else:
        out["r_next_close"] = None
    hour = [b for b in bars[idx:] if b[0] < entry_ts + 3600]
    out["max_up_60m"] = max(b[2] for b in hour) / entry - 1
    out["max_down_60m"] = min(b[3] for b in hour) / entry - 1
    out["max_up_day"] = max(b[2] for b in day_bars) / entry - 1
    for tp, sl in TRADE_TP_SL:
        out[f"tp{int(tp * 100)}_sl{int(sl * 100)}"] = tp_sl(day_bars, entry, entry_ts, tp, sl)
    return out


def parse_yahoo_chart(data: dict[str, Any]) -> list[Bar]:
    res = ((data.get("chart") or {}).get("result") or [None])[0] or {}
    q = ((res.get("indicators") or {}).get("quote") or [{}])[0]
    bars = []
    for i, ts in enumerate(res.get("timestamp") or []):
        vals = [(q.get(k) or [None] * (i + 1))[i] for k in ("open", "high", "low", "close", "volume")]
        if None not in vals[:4]:
            bars.append((float(ts), vals[0], vals[1], vals[2], vals[3], vals[4] or 0))
    return sorted(set(bars))


PERF_COLUMNS = [("r_5m", "5 דק'"), ("r_30m", "30 דק'"), ("r_60m", "שעה"), ("r_close", "סגירה"),
                ("tp2_sl2", "יעד +2% / סטופ -2%")]
PUMP_HE = {"high": "🔴 סיכון גבוה", "medium": "🟠 סיכון בינוני", "none": "ללא אזהרה"}


def _pct(x: float) -> str:
    return f"{x * 100:+.1f}%"


def _usd(x: float) -> str:
    return f"{'-' if x < 0 else '+'}${abs(x):,.0f}"


def perf_line(values: list[float]) -> str:
    n = len(values)
    wins = sum(1 for v in values if v > 0)
    gross = sum(values) * PERF_POSITION_USD
    net = sum(v - PERF_COST for v in values) * PERF_POSITION_USD
    return (f"{n} עסקאות · הצלחה {wins * 100 // n}% · ממוצע {_pct(sum(values) / n)} · "
            f"ברוטו {_usd(gross)} · נטו {_usd(net)}")


def perf_summary_text(log_entries: list[dict[str, Any]]) -> str:
    done = [e for e in log_entries if isinstance(e.get("r"), dict) and e["r"].get("tradable")]
    if not done:
        return "📊 עדיין אין התראות שנמדדו. הדוח נבנה מההתראות שהבוט שולח מעכשיו."
    first = dt.datetime.fromtimestamp(min(e["t"] for e in done), eastern_tz()).strftime("%d.%m.%y")
    lines = [f"📊 <b>מצטבר מאז {first}</b>: קנייה 3 דקות אחרי כל התראה, "
             f"${PERF_POSITION_USD:,} לעסקה (נטו = אחרי {PERF_COST * 100:.0f}% עלויות מרווח והחלקה)"]
    for key, label in PERF_COLUMNS:
        vals = [e["r"][key] for e in done if e["r"].get(key) is not None]
        if vals:
            lines.append(f"• <b>{label}</b>: {perf_line(vals)}")
    lines.append("<b>לפי אזהרת פמפום</b> (עד הסגירה):")
    for level, label in PUMP_HE.items():
        vals = [e["r"]["r_close"] for e in done if e.get("pump", "none") == level]
        if vals:
            lines.append(f"• {label}: {perf_line(vals)}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Parsing sources
# ---------------------------------------------------------------------------


@dataclass
class Filing:
    form: str
    company: str
    ciks: list[int]
    accession: str
    link: str
    items: list[str]
    filed_ts: float | None


_EDGAR_TITLE = re.compile(r"^\s*(\S+)\s+-\s+(.*?)\s+\((\d{4,10})\)\s*(?:\((\w+)\))?\s*$")


def parse_edgar_feed(content: bytes | str) -> list[Filing]:
    feed = feedparser.parse(content)
    out: dict[str, Filing] = {}
    for e in feed.entries:
        m = _EDGAR_TITLE.match(e.get("title", ""))
        if not m:
            continue
        form, company, cik = m.group(1).upper(), m.group(2), int(m.group(3))
        link = e.get("link", "")
        acc_m = re.search(r"accession-number=([\d-]+)", e.get("id", "")) or re.search(
            r"(\d{10}-\d{2}-\d{6})", link)
        if not acc_m:
            continue
        accession = acc_m.group(1)
        if accession in out:
            if cik not in out[accession].ciks:
                out[accession].ciks.append(cik)
            continue
        summary = e.get("summary", "")
        items = list(dict.fromkeys(re.findall(r"Item\s+(\d+\.\d+)", summary)))
        ts = _struct_ts(e.get("updated_parsed") or e.get("published_parsed"))
        out[accession] = Filing(form, company, [cik], accession, link, items, ts)
    return list(out.values())


def pick_document(index_html: str, base_url: str) -> tuple[str, bool] | None:
    """From an EDGAR filing index page, pick the press release (EX-99.x) if present,
    otherwise the main 8-K/6-K document. Returns (url, is_exhibit)."""
    best: tuple[tuple[int, float], str, bool] | None = None
    for row in re.findall(r"(?is)<tr[^>]*>(.*?)</tr>", index_html):
        cells = re.findall(r"(?is)<td[^>]*>(.*?)</td>", row)
        if len(cells) < 4:
            continue
        href_m = re.search(r'(?i)href="([^"]+)"', row)
        if not href_m:
            continue
        href = re.sub(r"^/ix\?doc=", "", href_m.group(1))
        if not re.search(r"\.(?:htm|html|txt)$", href, I):
            continue
        doc_type = html_to_text(cells[3]).strip().upper()
        ex = re.match(r"EX-99(?:\.(\d+))?", doc_type)
        if ex:
            rank = (0, float(ex.group(1) or 0))
            is_exhibit = True
        elif re.fullmatch(r"(?:8-K|6-K)(?:/A)?", doc_type):
            rank = (1, 0.0)
            is_exhibit = False
        else:
            continue
        if best is None or rank < best[0]:
            best = (rank, urljoin(base_url, href), is_exhibit)
    return (best[1], best[2]) if best else None


@dataclass
class WireItem:
    key: str
    title: str
    link: str
    summary: str
    published_ts: float | None
    tickers: list[str]


def parse_wire_feed(content: bytes | str) -> list[WireItem]:
    feed = feedparser.parse(content)
    items: list[WireItem] = []
    for e in feed.entries:
        title = html_to_text(e.get("title", ""))
        link = e.get("link", "")
        summary = html_to_text(e.get("summary", "") or e.get("description", ""))
        tags = " ".join(t.get("term", "") for t in e.get("tags", []) or [])
        key = e.get("id") or link
        if not key:
            continue
        ts = _struct_ts(e.get("published_parsed") or e.get("updated_parsed"))
        tickers = extract_tickers(f"{title}\n{summary}\n{tags}")
        items.append(WireItem(key, title, link, summary, ts, tickers))
    return items


def wire_source_name(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    for needle, name in (("prnewswire", "PR Newswire"), ("globenewswire", "GlobeNewswire"),
                         ("businesswire", "Business Wire"), ("accessnewswire", "ACCESS Newswire")):
        if needle in host:
            # Category feeds, e.g. ".../health-latest-news/biotechnology-list.rss" -> "biotechnology".
            leaf = parsed.path.rsplit("/", 1)[-1]
            m = re.fullmatch(r"(.+?)(?:-latest-news)?-list\.rss", leaf)
            if needle == "prnewswire" and m and m.group(1) != "news-releases":
                return f"{name} · {m.group(1)}"
            industry = re.search(r"/industry/\d+-([^/]+)", parsed.path)
            if needle == "globenewswire" and industry:
                return f"{name} · {unquote(industry.group(1))}"
            return name
    return host or url


# ---------------------------------------------------------------------------
# Infrastructure: HTTP fetcher, ticker map, state, Telegram
# ---------------------------------------------------------------------------


class SecBlocked(Exception):
    pass


def describe_error(exc: Exception) -> str:
    """Short, readable error text (httpx timeouts have an empty str())."""
    if isinstance(exc, httpx.HTTPStatusError):
        location = exc.response.headers.get("Location")
        return f"HTTP {exc.response.status_code}" + (f" → {location}" if location else "")
    if isinstance(exc, httpx.TimeoutException):
        return f"timeout ({type(exc).__name__})"
    return str(exc).split("\n")[0] or type(exc).__name__


class Fetcher:
    """GET with ETag / If-Modified-Since, SEC rate limiting and SEC block handling."""

    def __init__(self, client: httpx.AsyncClient, sec_user_agent: str,
                 wire_user_agent: str = WIRE_USER_AGENT):
        self.client = client
        self.sec_user_agent = sec_user_agent
        self.wire_user_agent = wire_user_agent
        self.validators: dict[str, tuple[str | None, str | None]] = {}
        self.sec_blocked_until = 0.0
        self._sec_lock = asyncio.Lock()
        self._sec_last = 0.0

    @staticmethod
    def is_sec(url: str) -> bool:
        return urlparse(url).netloc.lower().endswith("sec.gov")

    async def get(self, url: str, conditional: bool = False, follow_redirects: bool = True,
                  timeout: float | None = None) -> httpx.Response | None:
        """Returns the response, or None when a conditional request got 304."""
        sec = self.is_sec(url)
        headers = {"User-Agent": self.sec_user_agent} if sec else {"User-Agent": self.wire_user_agent, **WIRE_HEADERS}
        if conditional and url in self.validators:
            etag, modified = self.validators[url]
            if etag:
                headers["If-None-Match"] = etag
            if modified:
                headers["If-Modified-Since"] = modified
        if sec:
            if time.time() < self.sec_blocked_until:
                raise SecBlocked()
            async with self._sec_lock:
                wait = self._sec_last + SEC_MIN_INTERVAL - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
                self._sec_last = time.monotonic()
        kwargs: dict[str, Any] = {"timeout": timeout} if timeout else {}
        resp = await self.client.get(url, headers=headers, follow_redirects=follow_redirects, **kwargs)
        if resp.status_code == 304:
            return None
        if sec and resp.status_code in (403, 429):
            self.sec_blocked_until = time.time() + SEC_BLOCK_SECONDS
            log.warning("SEC returned %s — pausing SEC requests for 10 minutes", resp.status_code)
            raise SecBlocked()
        resp.raise_for_status()
        if conditional:
            self.validators[url] = (resp.headers.get("ETag"), resp.headers.get("Last-Modified"))
        return resp


_NAME_SUFFIXES = {"inc", "incorporated", "corp", "corporation", "co", "company", "ltd", "limited",
                  "plc", "llc", "lp", "sa", "nv", "ag", "se", "holdings", "holding", "group", "the",
                  "de", "del", "adr", "ads", "class"}
GENERIC_NAMES = {"american", "global", "first", "united", "national", "international", "general",
                 "new", "china", "digital", "capital", "energy", "health", "bank"}


def normalize_company(name: str) -> str:
    words = re.findall(r"[a-z0-9]+", name.lower().replace("&", " and "))
    return " ".join(w for w in words if w not in _NAME_SUFFIXES)


class TickerMap:
    def __init__(self) -> None:
        self.by_ticker: dict[str, tuple[int, str]] = {}
        self.by_cik: dict[int, list[str]] = {}
        self.by_name: dict[str, str] = {}  # normalized company name -> primary ticker
        self.loaded_at = 0.0

    @property
    def loaded(self) -> bool:
        return bool(self.by_ticker)

    def _add(self, cik: int, name: str, ticker: str) -> None:
        t = normalize_ticker(ticker)
        if not t or t in self.by_ticker:
            return
        self.by_ticker[t] = (cik, name)
        self.by_cik.setdefault(cik, []).append(t)
        key = normalize_company(name)
        if len(key) >= 4 and key not in GENERIC_NAMES:
            self.by_name.setdefault(key, t)

    def load(self, data: Any) -> None:
        self.by_ticker, self.by_cik, self.by_name = {}, {}, {}
        if isinstance(data, dict) and "fields" in data:  # company_tickers_exchange.json
            f = {name: i for i, name in enumerate(data["fields"])}
            for row in data["data"]:
                exchange = (row[f["exchange"]] or "").strip()
                if not exchange or exchange.upper() == "OTC":
                    continue  # US exchange-listed stocks only
                self._add(int(row[f["cik"]]), row[f["name"]], row[f["ticker"]])
        else:  # company_tickers.json
            for v in data.values():
                self._add(int(v["cik_str"]), v["title"], v["ticker"])
        self.loaded_at = time.time()

    def lookup(self, ticker: str) -> tuple[int, str] | None:
        return self.by_ticker.get(normalize_ticker(ticker))

    def tickers_for_cik(self, cik: int) -> list[str]:
        return self.by_cik.get(cik, [])

    def match_title(self, title: str) -> str | None:
        """Ticker of a listed company whose full name starts the headline, e.g.
        "Kirby Corporation Announces ..." -> KEX. Longest match wins; exact names only."""
        words = re.findall(r"[\w&'.-]+", title)[:8]
        for n in range(len(words), 0, -1):
            key = normalize_company(" ".join(words[:n]))
            if key in self.by_name:
                return self.by_name[key]
        return None


class State:
    """Persistent state, written atomically through a temp file."""

    def __init__(self, path: Path):
        self.path = path
        self.watchlist: list[str] = []
        self.seen: dict[str, None] = {}
        self.initialized: set[str] = set()
        self.last_alert: dict[str, float] = {}
        self.tg_offset = 0
        self.sources: dict[str, dict[str, Any]] = {}
        self.catalysts: dict[str, dict[str, Any]] = {}  # "TICKER:YYYY-MM-DD" -> details
        self.gurus: dict[str, str] = {}                  # CIK -> accession of the last 13F reported
        self.news_log: dict[str, list[dict[str, Any]]] = {}  # ticker -> news seen in the last 72h
        self.gainers_log: list[dict[str, Any]] = []      # daily study of the biggest gainers
        self.gainers_day = ""
        self.learning_ready_sent = False
        self.news_log_since = 0.0                        # when news logging started (first full day only)
        self.momentum: dict[str, Any] = {"date": "", "tickers": []}  # breakout alerts sent today
        self.guru_checked = 0.0
        self.cusip_tickers: dict[str, str] = {}          # CUSIP -> ticker ("" = none found)
        self.alert_log: list[dict[str, Any]] = []       # every alert sent, for the performance report
        self.perf_day = ""                               # last day the performance report went out
        self.dirty = False

    @classmethod
    def load(cls, path: Path, seed_watchlist: list[str]) -> "State":
        st = cls(path)
        data: dict[str, Any] = {}
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                log.error("Could not read %s (%s) — starting with a fresh state", path, exc)
        if "watchlist" in data:
            st.watchlist = [normalize_ticker(t) for t in data["watchlist"]]
        else:
            st.watchlist = list(dict.fromkeys(seed_watchlist))
            st.dirty = True
        st.seen = dict.fromkeys(data.get("seen", []))
        st.initialized = set(data.get("initialized", []))
        st.last_alert = {k: float(v) for k, v in data.get("last_alert", {}).items()}
        st.tg_offset = int(data.get("tg_offset", 0))
        st.sources = data.get("sources", {})
        st.catalysts = data.get("catalysts", {})
        st.gurus = data.get("gurus", {})
        st.news_log = data.get("news_log", {})
        st.gainers_log = data.get("gainers_log", [])
        st.gainers_day = data.get("gainers_day", "")
        st.learning_ready_sent = bool(data.get("learning_ready_sent", False))
        st.news_log_since = float(data.get("news_log_since", 0.0))
        st.momentum = data.get("momentum", {"date": "", "tickers": []})
        st.guru_checked = float(data.get("guru_checked", 0.0))
        st.cusip_tickers = data.get("cusip_tickers", {})
        st.alert_log = data.get("alert_log", [])
        if "alert_log" not in data:  # first run with the report: seed it from the last week's alerts
            st.alert_log = sorted(({"t": ts, "ticker": k, "score": None, "pump": "unknown", "src": "", "title": ""}
                                   for k, ts in st.last_alert.items() if re.fullmatch(r"[A-Z][A-Z0-9\-]{0,9}", k)),
                                  key=lambda e: e["t"])
        st.perf_day = data.get("perf_day", "")
        return st

    def pruned_news_log(self) -> dict[str, list[dict[str, Any]]]:
        cutoff = time.time() - NEWS_LOG_HOURS * 3600
        self.news_log = {t: kept for t, items in self.news_log.items()
                         if (kept := [i for i in items if i["t"] >= cutoff])}
        return self.news_log

    def log_news(self, ticker: str, src: str, title: str, pub: float | None) -> None:
        if not self.news_log_since:
            self.news_log_since = time.time()
        items = self.news_log.setdefault(ticker, [])
        key = (title or "")[:60].lower()
        if any(i["title"][:60].lower() == key for i in items):
            return  # the same release from another feed
        items.append({"t": time.time(), "pub": pub, "src": src, "title": (title or "")[:140]})

    def is_seen(self, key: str) -> bool:
        return key in self.seen

    def mark_seen(self, key: str) -> None:
        if key in self.seen:
            return
        self.seen[key] = None
        while len(self.seen) > MAX_SEEN:
            del self.seen[next(iter(self.seen))]
        self.dirty = True

    def save(self) -> None:
        cutoff = time.time() - 7 * 86400
        self.last_alert = {k: v for k, v in self.last_alert.items() if v >= cutoff}
        stale = (us_eastern_now().date() - dt.timedelta(days=7)).isoformat()
        self.catalysts = {k: v for k, v in self.catalysts.items() if v.get("date", "") >= stale}
        data = {
            "watchlist": self.watchlist,
            "initialized": sorted(self.initialized),
            "last_alert": self.last_alert,
            "tg_offset": self.tg_offset,
            "sources": self.sources,
            "catalysts": self.catalysts,
            "gurus": self.gurus,
            "news_log": self.pruned_news_log(),
            "gainers_log": self.gainers_log[-GAINERS_LOG_MAX:],
            "gainers_day": self.gainers_day,
            "learning_ready_sent": self.learning_ready_sent,
            "news_log_since": self.news_log_since,
            "momentum": self.momentum,
            "guru_checked": self.guru_checked,
            "cusip_tickers": self.cusip_tickers,
            "alert_log": self.alert_log[-ALERT_LOG_MAX:],
            "perf_day": self.perf_day,
            "seen": list(self.seen),
        }
        if self.path.parent and not self.path.parent.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, self.path)
        self.dirty = False


class TelegramError(Exception):
    def __init__(self, status: int, description: str):
        super().__init__(f"{status}: {description}")
        self.status = status
        self.description = description


class Telegram:
    def __init__(self, client: httpx.AsyncClient, token: str):
        self.client = client
        self.token = token

    async def call(self, method: str, payload: dict[str, Any] | None = None,
                   timeout: float = 20.0) -> Any:
        url = TELEGRAM_API.format(token=self.token, method=method)
        last_error: Exception | None = None
        for attempt in range(5):
            try:
                resp = await self.client.post(url, json=payload or {}, timeout=timeout)
            except httpx.HTTPError as exc:
                last_error = exc
                await asyncio.sleep(min(2 ** attempt, 10))
                continue
            try:
                data = resp.json()
            except ValueError:
                data = {"ok": False, "description": resp.text[:200]}
            if resp.status_code == 429:
                retry_after = (data.get("parameters") or {}).get("retry_after", 5)
                log.warning("Telegram 429 — retrying after %ss", retry_after)
                await asyncio.sleep(float(retry_after) + 0.5)
                continue
            if resp.status_code >= 500:
                last_error = TelegramError(resp.status_code, str(data.get("description")))
                await asyncio.sleep(min(2 ** attempt, 10))
                continue
            if not data.get("ok"):
                raise TelegramError(resp.status_code, str(data.get("description", "")))
            return data.get("result")
        if isinstance(last_error, TelegramError):
            raise last_error
        raise TelegramError(0, f"network error: {last_error}")

    async def send(self, chat_id: str, text: str) -> Any:
        return await self.call("sendMessage", {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "link_preview_options": {"is_disabled": True},
        })


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------


@dataclass
class Candidate:
    source: str            # "sec" or "wire"
    source_label: str      # e.g. "SEC EDGAR · 8-K" / "PR Newswire"
    ticker: str | None
    company: str
    title: str
    link: str
    published_ts: float | None
    watch: bool
    form: str = ""
    items: list[str] = field(default_factory=list)
    summary: str = ""
    text: str | None = None     # fetched text, cached between steps

    @property
    def dedup_key(self) -> str:
        return self.ticker or f"{self.source}:{self.company}"


def score_tag(score: int | None) -> str:
    if score is None:
        return "📄 דיווח חדש (רשימת מעקב)"
    if score >= 5:
        return f"🚀🚀 חיובי מאוד (+{score})"
    if score >= 4:
        return f"🚀 חיובי מאוד (+{score})"
    if score >= 1:
        return f"📈 חיובי (+{score})"
    if score == 0:
        return "➖ ניטרלי (0)"
    return f"🔻 שלילי ({score})"


def format_alert(c: Candidate, score: int | None, reason: str, now: float | None = None,
                 risk: PumpRisk | None = None, odds: "JumpOdds | None" = None) -> str:
    now = time.time() if now is None else now
    lines = [score_tag(score)]
    if odds and odds.golden:
        lines.insert(0, "🏆 ידיעת זהב: חדשות מהסוג שהקפיץ מניות בעבר")
    head = f"<b>{esc(c.ticker or '—')}</b>"
    if c.company:
        head += f" | {esc(c.company)}"
    lines.append(head)
    if c.source == "sec":
        if c.items:
            lines.extend(f"Item {it}: {esc(ITEMS_HE.get(it, ''))}".rstrip(": ") for it in c.items)
        else:
            lines.append(f"{esc(c.form)}: דיווח שוטף של חברה זרה" if c.form == "6-K" else esc(c.form))
    else:
        lines.append(esc(c.title))
    if reason:
        lines.append(f"💡 {esc(reason)}")
    if odds:
        line = (f"🎯 סיכוי היסטורי לקפיצה של 20%+: {odds.probability * 100:.0f}% "
                f"(פי {odds.probability / odds.base:.1f} מהודעה רגילה)" if odds.base else "")
        if odds.reasons:
            line += " · " + ", ".join(esc(r) for r in odds.reasons)
        lines.append(line)
    if risk and risk.level:
        tag = "🔴 סיכון פמפום גבוה" if risk.level == "high" else "🟠 סיכון פמפום בינוני"
        lines.append(f"⚠️ {tag}: " + "; ".join(esc(r) for r in risk.reasons))
    source_line = f"📰 {esc(c.source_label)}"
    if c.published_ts is not None:
        age = now - c.published_ts
        if -120 <= age < ALERT_AGE_LIMIT:
            source_line += f" · ⏱ {fmt_duration(age)} מהפרסום"
    lines.append(source_line)
    lines.append(f'<a href="{html.escape(c.link, quote=True)}">למקור המלא</a>')
    return "\n".join(lines)


SAMPLE_CANDIDATE = Candidate(
    source="wire", source_label="PR Newswire", ticker="OKLO", company="Oklo Inc.",
    title="Oklo Awarded $450 Million Contract by U.S. Department of Defense",
    link="https://www.prnewswire.com/", published_ts=None, watch=False,
)


def sample_alert() -> str:
    c = Candidate(**{**SAMPLE_CANDIDATE.__dict__, "published_ts": time.time() - 8})
    return "🧪 התראת דוגמה\n\n" + format_alert(c, 5, "חוזה ענק ביחס לשווי החברה מול לקוח ממשלתי")


# ---------------------------------------------------------------------------
# The radar
# ---------------------------------------------------------------------------


class Radar:
    def __init__(self, cfg: Config, client: httpx.AsyncClient, once: bool = False):
        self.cfg = cfg
        self.client = client
        self.once = once
        self.state = State.load(cfg.state_file, cfg.watchlist)
        self.fetcher = Fetcher(client, cfg.sec_user_agent, cfg.wire_user_agent)
        self.tickers = TickerMap()
        self.tg = Telegram(client, cfg.telegram_token)
        self.chat_id = cfg.chat_id
        self.ai_sem = asyncio.Semaphore(AI_CONCURRENCY)
        self.tasks: set[asyncio.Task[Any]] = set()
        self.started = time.time()
        self.stats = {"checked": 0, "candidates": 0, "ai_calls": 0, "ai_errors": 0, "alerts": 0}
        self.stop_event = asyncio.Event()
        self.pending_status = False
        self.prices: dict[str, tuple[float, float, float]] = {}    # ticker -> (fetched at, price, 5-day change)
        self.sec_cache: dict[str, tuple[float, Any]] = {}
        self.jump_model = load_jump_model() if cfg.jump_model else None
        self.momentum_checked: dict[str, float] = {}
        self.momentum_scan_at = 0.0
        # --once handles ~5 minutes of news per pass, so it gets a larger per-source cap.
        self.max_per_cycle = cfg.max_per_cycle * (4 if once else 1)  # --once: answer /status after polling, with fresh data

    # ----- source status -------------------------------------------------

    def _source_ok(self, name: str) -> None:
        prev = self.state.sources.get(name, {})
        self.state.sources[name] = {**prev, "ok": True, "last_ok": time.time(), "error": ""}

    def _source_error(self, name: str, error: str) -> None:
        prev = self.state.sources.get(name, {})
        self.state.sources[name] = {**prev, "ok": False, "error": error[:200]}

    def _check_overflow(self, name: str, keys: list[str], first: bool) -> None:
        """Every item in the feed is new → older items scrolled out of the feed before we
        read them, i.e. news may have been missed. Counted per source for /status."""
        if first or not keys or any(self.state.is_seen(k) for k in keys):
            return
        entry = self.state.sources.setdefault(name, {})
        entry["overflows"] = int(entry.get("overflows", 0)) + 1
        entry["last_overflow"] = time.time()
        self.state.dirty = True
        log.warning("%s: all %d feed items are new — items may have been missed since the last poll",
                    name, len(keys))

    # ----- tickers -------------------------------------------------------

    async def refresh_tickers(self) -> bool:
        for url in (TICKERS_EXCHANGE_URL, TICKERS_URL):
            try:
                resp = await self.fetcher.get(url)
                assert resp is not None
                self.tickers.load(resp.json())
                self._source_ok("SEC Tickers")
                log.info("Ticker map loaded from %s: %d tickers", url, len(self.tickers.by_ticker))
                return True
            except SecBlocked:
                self._source_error("SEC Tickers", "חסימת SEC — ממתין 10 דקות")
                return False
            except Exception as exc:  # noqa: BLE001
                log.warning("Ticker map %s failed: %s", url, exc)
                self._source_error("SEC Tickers", str(exc))
        return False

    # ----- EDGAR ---------------------------------------------------------

    async def poll_edgar(self, form: str) -> None:
        name = f"SEC {form}"
        url = EDGAR_FEED_URL.format(form=quote(form))
        try:
            # SEC's live feed answers in under a second or hangs: give up quickly, retry once.
            try:
                resp = await self.fetcher.get(url, conditional=True, timeout=FEED_TIMEOUT)
            except httpx.TransportError:
                await asyncio.sleep(1)
                resp = await self.fetcher.get(url, conditional=True, timeout=FEED_TIMEOUT)
        except SecBlocked:
            self._source_error(name, "חסימת SEC — ממתין 10 דקות")
            return
        except Exception as exc:  # noqa: BLE001
            log.warning("%s poll failed: %s", name, describe_error(exc))
            self._source_error(name, describe_error(exc))
            return
        self._source_ok(name)
        if resp is None:
            return
        filings = parse_edgar_feed(resp.content)
        init_key = f"edgar:{form}"
        first = init_key not in self.state.initialized
        self._check_overflow(name, [f"sec:{f.accession}" for f in filings], first)
        dispatched = 0
        for f in reversed(filings):  # oldest first
            key = f"sec:{f.accession}"
            if self.state.is_seen(key):
                continue
            self.state.mark_seen(key)
            if first or f.form != form:
                continue
            self.stats["checked"] += 1
            cand = self.edgar_candidate(f)
            for t in (self.tickers.tickers_for_cik(f.ciks[0])[:1] if f.ciks else []):
                self.state.log_news(t, f"SEC {f.form}", f"{f.form} " + ", ".join(f.items), f.filed_ts)
            if cand is None:
                continue
            if dispatched >= self.max_per_cycle:
                log.warning("%s: over MAX_ALERTS_PER_CYCLE, skipping %s", name, f.accession)
                continue
            dispatched += 1
            self.spawn(self.process(cand))
        if first:
            self.state.initialized.add(init_key)
            self.state.dirty = True
            log.info("%s initialized: %d existing filings marked as seen", name, len(filings))

    def edgar_candidate(self, f: Filing) -> Candidate | None:
        ticker, tickers = None, []
        for cik in f.ciks:
            tickers = self.tickers.tickers_for_cik(cik)
            if tickers:
                ticker = tickers[0]
                break
        if self.tickers.loaded and not ticker:
            return None  # not a stock listed on a US exchange
        watch = any(t in self.state.watchlist for t in tickers)
        if not watch:
            if not self.cfg.marketwide:
                return None
            if f.form == "8-K" and not set(f.items) & self.cfg.candidate_items:
                return None
        company = self.tickers.lookup(ticker)[1] if ticker else f.company
        return Candidate(
            source="sec", source_label=f"SEC EDGAR · {f.form}", ticker=ticker, company=company,
            title="", link=f.link, published_ts=f.filed_ts, watch=watch, form=f.form,
            items=f.items,
        )

    # ----- wires ---------------------------------------------------------

    async def fetch_wire_feed(self, url: str, conditional: bool = True) -> httpx.Response | None:
        """GET a wire RSS feed with retries. Redirects are NOT followed: PR Newswire
        intermittently 301s to a URL that 404s, while retrying the feed URL works."""
        for attempt in range(1, WIRE_ATTEMPTS + 1):
            try:
                return await self.fetcher.get(url, conditional=conditional, follow_redirects=False,
                                              timeout=FEED_TIMEOUT)
            except Exception as exc:  # noqa: BLE001
                status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else 0
                retryable = isinstance(exc, httpx.TransportError) or 300 <= status < 400 or status >= 500
                if not retryable or attempt == WIRE_ATTEMPTS:
                    raise
                log.info("%s: %s, retrying", wire_source_name(url), describe_error(exc))
                await asyncio.sleep(2)
        return None  # unreachable

    async def poll_wire(self, url: str) -> None:
        name = wire_source_name(url)
        try:
            resp = await self.fetch_wire_feed(url)
        except Exception as exc:  # noqa: BLE001
            error = describe_error(exc)
            log.warning("%s poll failed: %s", name, error)
            self._source_error(name, error)
            return
        if resp is None:
            self._source_ok(name)
            return
        items = parse_wire_feed(resp.content)
        if not items:
            self._source_error(name, "הפיד ריק או לא תקין — בדוק את הכתובת ב-WIRE_FEEDS")
            return
        self._source_ok(name)
        init_key = f"wire:{url}"
        first = init_key not in self.state.initialized
        self._check_overflow(name, [f"wire:{it.key}" for it in items], first)
        dispatched = 0
        for it in reversed(items):
            key = f"wire:{it.key}"
            if self.state.is_seen(key):
                continue
            self.state.mark_seen(key)
            if first:
                continue
            self.stats["checked"] += 1
            cand = self.wire_candidate(it, name)
            if cand and cand.ticker:
                self.state.log_news(cand.ticker, name, it.title, it.published_ts)
            if cand is None:
                continue
            if dispatched >= self.max_per_cycle:
                log.warning("%s: over MAX_ALERTS_PER_CYCLE, skipping %s", name, it.title)
                continue
            dispatched += 1
            self.spawn(self.process(cand))
        if first:
            self.state.initialized.add(init_key)
            self.state.dirty = True
            log.info("%s initialized: %d existing items marked as seen", name, len(items))

    def wire_candidate(self, it: WireItem, source_name: str) -> Candidate | None:
        # Ticker from the feed text, else from a listed company's name opening the headline
        # (Business Wire rarely puts the ticker in its feed).
        ticker = it.tickers[0] if it.tickers else self.tickers.match_title(it.title)
        if ticker is None:
            return None  # no US exchange ticker: private or non-US company
        watch = ticker in self.state.watchlist
        if not watch and not self.cfg.marketwide:
            return None
        found = self.tickers.lookup(ticker)
        return Candidate(
            source="wire", source_label=source_name, ticker=ticker,
            company=found[1] if found else "", title=it.title, link=it.link,
            published_ts=it.published_ts, watch=watch, summary=it.summary,
        )

    # ----- candidate pipeline ---------------------------------------------

    def spawn(self, coro: Any) -> None:
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        self.tasks.discard(task)
        if not task.cancelled() and task.exception():
            log.error("Candidate task failed", exc_info=task.exception())

    async def drain(self, timeout: float = 150.0) -> None:
        if self.tasks:
            _, pending = await asyncio.wait(set(self.tasks), timeout=timeout)
            for t in pending:
                t.cancel()

    async def fetch_text(self, c: Candidate) -> str:
        if c.text is None:
            c.text = await self._fetch_text(c)
        return c.text

    async def _fetch_text(self, c: Candidate) -> str:
        if c.source == "wire" and urlparse(c.link).netloc.lower().endswith(NO_PAGE_HOSTS):
            return c.summary
        if c.source == "sec":
            index = await self.fetcher.get(c.link)
            assert index is not None
            picked = pick_document(index.text, c.link)
            if not picked:
                return ""
            doc_url, is_exhibit = picked
            doc = await self.fetcher.get(doc_url)
            assert doc is not None
            text = html_to_text(doc.text)
            return text if is_exhibit else skip_cover_page(text)
        page = await self.fetcher.get(c.link)
        assert page is not None
        body = extract_article_text(page.text)
        return body or c.summary

    async def claude_score(self, c: Candidate, text: str) -> dict[str, Any] | None:
        payload = {
            "model": self.cfg.anthropic_model,
            "max_tokens": 300,
            "temperature": 0,
            "system": CLAUDE_SYSTEM,
            "messages": [{"role": "user", "content": (
                f"Company: {c.company or 'unknown'}\nTicker: {c.ticker or 'unknown'}\n"
                f"Source: {c.source_label}\n\n{text[:AI_TEXT_CHARS]}"
            )}],
        }
        headers = {
            "x-api-key": self.cfg.anthropic_key,
            "anthropic-version": ANTHROPIC_VERSION,
            "content-type": "application/json",
        }
        async with self.ai_sem:
            self.stats["ai_calls"] += 1
            try:
                resp = await self.client.post(ANTHROPIC_URL, json=payload, headers=headers, timeout=30.0)
                resp.raise_for_status()
                data = resp.json()
                text_out = "".join(b.get("text", "") for b in data.get("content", [])
                                   if b.get("type") == "text")
                result = parse_claude_json(text_out)
                if result is None:
                    raise ValueError(f"unparseable reply: {text_out[:200]!r}")
                return result
            except Exception as exc:  # noqa: BLE001
                self.stats["ai_errors"] += 1
                log.warning("Claude scoring failed for %s, falling back to rules: %s", c.ticker, exc)
                return None

    async def evaluate(self, c: Candidate) -> tuple[int, str, str | None, Catalyst | None]:
        """Fetch the text and score it.
        Returns (score, reason_he, rejected_label, upcoming catalyst announced in the item)."""
        try:
            text = await self.fetch_text(c)
        except Exception as exc:  # noqa: BLE001
            log.warning("Fetching text for %s failed: %s", c.link, exc)
            text = c.summary
        body = strip_boilerplate(text)
        neg = negative_hit(f"{c.title}\n{body[:NEGATIVE_CHARS]}")
        if neg:
            return -5, neg, neg, None
        rules = rule_score(f"{c.title}\n{body[:LEAD_CHARS]}", c.company,
                           strong_text=f"{c.title}\n{body[:STRONG_CHARS]}", title=c.title)
        catalyst = None
        if rules.score < 4:  # a strong item is the result itself, not an announcement of one
            catalyst = find_catalyst(f"{c.title}\n{body[:LEAD_CHARS]}", us_eastern_now().date())
        score, reason = rules.score, rules.reason_he
        if 2 <= score < self.cfg.min_score and c.ticker and not self.cfg.anthropic_key:
            price = await self.last_price(c.ticker)
            if price is not None and price < SMALL_CAP_PRICE:
                score += 1
                reason = ", ".join(x for x in (reason, f"מניה זולה (${price:.2f}): חדשות כאלה מזיזות מניות קטנות") if x)
        if self.cfg.anthropic_key and (rules.score >= 1 or c.watch):
            head = f"{c.title}\n{body}" if c.title else body
            ai = await self.claude_score(c, head)
            if ai is not None:
                score = ai["score"]
                reason = ai["reason_he"] or reason
                if not c.ticker and ai["ticker"]:
                    c.ticker = normalize_ticker(ai["ticker"])
        return score, reason, None, catalyst

    async def last_price(self, ticker: str) -> float | None:
        quote_ = await self.daily_quote(ticker)
        return quote_[0] if quote_ else None

    async def daily_quote(self, ticker: str) -> tuple[float, float] | None:
        """(latest price, change over the 5 sessions before today) from Yahoo, cached 10 minutes."""
        cached = self.prices.get(ticker)
        if cached and time.time() - cached[0] < 600:
            return cached[1], cached[2]
        try:
            resp = await self.client.get(YAHOO_DAILY_URL.format(symbol=ticker), headers=RESEARCH_HEADERS, timeout=4)
            res = ((resp.json().get("chart") or {}).get("result") or [{}])[0]
            price = float((res.get("meta") or {})["regularMarketPrice"])
            today = us_eastern_now().date()
            closes = [c for t, c in zip(res.get("timestamp") or [], ((res.get("indicators") or {}).get("quote")
                                                                      or [{}])[0].get("close") or [])
                      if c and dt.datetime.fromtimestamp(t, eastern_tz()).date() < today][-6:]
            pre5 = closes[-1] / closes[0] - 1 if len(closes) >= 2 and closes[0] > 0 else 0.0
        except Exception as exc:  # noqa: BLE001
            log.info("Price for %s unavailable: %s", ticker, describe_error(exc))
            return None
        self.prices[ticker] = (time.time(), price, pre5)
        return price, pre5

    async def sec_json(self, url: str, max_age: float = 3600) -> Any:
        """A small SEC JSON document (submissions, share count), cached; None when unavailable."""
        cached = self.sec_cache.get(url)
        if cached and time.time() - cached[0] < max_age:
            return cached[1]
        try:
            resp = await self.fetcher.get(url, timeout=4.0)
            data = resp.json() if resp is not None and resp.status_code == 200 else None
        except Exception as exc:  # noqa: BLE001
            log.info("SEC %s unavailable: %s", url, describe_error(exc))
            return None
        if len(self.sec_cache) > 500:
            self.sec_cache.clear()
        self.sec_cache[url] = (time.time(), data)
        return data

    async def jump_odds(self, c: Candidate, score: int | None, risk: PumpRisk | None) -> JumpOdds | None:
        """The past year's jump rate for news like this one (data/jump_model.json); None without a model."""
        model = self.jump_model
        found = self.tickers.lookup(c.ticker) if c.ticker else None
        if not model or not found or score is None:
            return None
        cik = found[0]
        quote_, subs, facts = await asyncio.gather(
            self.daily_quote(c.ticker or ""), self.sec_json(SEC_SUBMISSIONS_URL.format(cik=cik)),
            self.sec_json(SEC_SHARES_URL.format(cik=cik), max_age=86400))
        price, pre5 = quote_ or (None, 0.0)
        units = ((facts or {}).get("units") or {}).get("shares") or []
        shares = float(max(units, key=lambda u: u.get("end", ""))["val"]) if units else None
        mcap = price * shares if price and shares else None
        try:
            sic = int((subs or {}).get("sic") or 0)
        except ValueError:
            sic = 0
        body = strip_boilerplate(c.text or c.summary)
        lead = body[:300]
        head = c.title if c.source == "wire" else release_headline(lead)
        text = f"{head}\n{lead}"
        row = {"cat": classify_catalyst(text), "cap": cap_bucket(mcap), "price": price_bucket(price),
               "sector": sector_of(sic), "session": session_of(time.time()), "pump": (risk.level if risk else "") or "none",
               "amount": amount_bucket(text, mcap), "score": score, "pre5": pre5, "headline": head, "lead": lead}
        feats = jump_features(row, model["vocab_set"])
        weights = model["weights"]
        return JumpOdds(jump_probability(weights, feats), float(model.get("base_rate", 0.05)),
                        explain_features(weights, feats))

    async def process(self, c: Candidate) -> None:
        self.stats["candidates"] += 1
        score: int | None = None
        reason = ""
        if self.cfg.positive_only or not c.watch:
            score, reason, rejected, catalyst = await self.evaluate(c)
            if catalyst and self.cfg.catalyst_alerts:
                await self.notify_catalyst(c, catalyst)
            if rejected:
                log.info("Rejected %s (%s): %s", c.ticker, rejected, c.title or c.items)
                return
            if score < self.cfg.min_score and not (self.jump_model and score >= 0 and c.ticker):
                log.info("Below threshold %s (%s): %s", c.ticker, score, c.title or c.items)
                return
        now = time.time()
        key = c.dedup_key
        last = self.state.last_alert.get(key)
        if last is not None and now - last < self.cfg.dedup_hours * 3600:
            log.info("Duplicate for %s within %sh — skipped", key, self.cfg.dedup_hours)
            return
        if not self.chat_id:
            log.warning("No TELEGRAM_CHAT_ID yet — cannot send alert for %s", key)
            return
        risk = await self.pump_risk(c) if self.cfg.pump_check and score is not None else None
        odds = await self.jump_odds(c, score, risk) if self.jump_model and score is not None else None
        if score is not None and not self.should_alert(score, odds):
            log.info("Below threshold %s (%s, jump odds %s): %s", c.ticker, score,
                     f"{odds.probability:.0%}" if odds else "—", c.title or c.items)
            return
        last = self.state.last_alert.get(key)
        if last is not None and time.time() - last < self.cfg.dedup_hours * 3600:   # alerted meanwhile
            return
        self.state.last_alert[key] = now
        self.state.dirty = True
        try:
            await self.tg.send(self.chat_id, format_alert(c, score, reason, now, risk, odds))
            self.stats["alerts"] += 1
            log.info("ALERT %s score=%s jump=%s %s", key, score, f"{odds.probability:.2f}" if odds else "-",
                     c.title or c.items)
            self.state.alert_log.append({
                "t": now, "ticker": c.ticker, "score": score, "pump": (risk.level if risk else "") or "none",
                "src": c.source_label, "title": (c.title or ", ".join(c.items) or c.form)[:100],
                **({"jump": round(odds.probability, 3)} if odds else {}),
            })
        except Exception as exc:  # noqa: BLE001
            if self.state.last_alert.get(key) == now:
                del self.state.last_alert[key]
            log.error("Sending alert for %s failed: %s", key, exc)

    def should_alert(self, score: int, odds: JumpOdds | None) -> bool:
        """Rules decide; the jump model, when present, adds golden news the rules scored low and
        drops high-scored news that historically almost never moved the stock."""
        if odds is None or not self.jump_model:
            return score >= self.cfg.min_score
        if odds.probability >= float(self.jump_model.get("promote_at", 2.0)):
            return True
        if odds.probability < float(self.jump_model.get("mute_below", 0.0)):
            return False
        return score >= self.cfg.min_score

    async def pump_risk(self, c: Candidate) -> PumpRisk | None:
        """Crash-risk warning for an alert. Never delays an alert by more than a few seconds."""
        text = f"{c.title}\n{strip_boilerplate(c.text or c.summary)[:LEAD_CHARS]}"
        found = self.tickers.lookup(c.ticker) if c.ticker else None
        submissions = await self.sec_json(SEC_SUBMISSIONS_URL.format(cik=found[0])) if found else None
        risk = assess_pump_risk(text, submissions, us_eastern_now().date())
        if risk.level:
            log.info("Pump risk %s for %s: %s", risk.level, c.ticker, "; ".join(risk.reasons))
        return risk

    # ----- catalysts -------------------------------------------------------

    async def notify_catalyst(self, c: Candidate, cat: Catalyst) -> None:
        """Heads-up for a scheduled catalyst; the ticker joins the watchlist."""
        if not c.ticker:
            return
        key = f"{c.ticker}:{cat.date.isoformat()}"
        if key in self.state.catalysts:
            return
        added = c.ticker not in self.state.watchlist
        if added:
            self.state.watchlist.append(c.ticker)
        self.state.catalysts[key] = {
            "ticker": c.ticker, "company": c.company, "date": cat.date.isoformat(),
            "label": cat.label, "title": c.title or ", ".join(f"Item {i}" for i in c.items),
            "link": c.link, "source": c.source_label, "notified": time.time(), "reminded": False,
        }
        self.state.dirty = True
        today = us_eastern_now().date()
        lines = [
            "📅 <b>קטליזטור צפוי</b>",
            f"<b>{esc(c.ticker)}</b>" + (f" | {esc(c.company)}" if c.company else ""),
            f"🗓 {esc(cat.label)} · {hebrew_date(cat.date)} ({days_until_he(cat.date, today)})",
        ]
        if c.title:
            lines.append(esc(c.title))
        lines.append("➕ נוספה לרשימת המעקב" if added else "👁 כבר ברשימת המעקב")
        lines.append(f"📰 {esc(c.source_label)}")
        lines.append(f'<a href="{html.escape(c.link, quote=True)}">למקור המלא</a>')
        log.info("CATALYST %s %s %s", c.ticker, cat.date, cat.label)
        await self.reply("\n".join(lines))

    async def check_catalyst_reminders(self) -> None:
        """On the catalyst day (from 04:00 New York, when pre-market opens) send one reminder."""
        now = us_eastern_now()
        if now.hour < 4:
            return
        today = now.date().isoformat()
        for entry in self.state.catalysts.values():
            if entry.get("date") != today or entry.get("reminded"):
                continue
            entry["reminded"] = True
            self.state.dirty = True
            lines = [
                "⏰ <b>היום: קטליזטור צפוי</b>",
                f"<b>{esc(entry['ticker'])}</b>" + (f" | {esc(entry['company'])}" if entry.get("company") else ""),
                f"🗓 {esc(entry['label'])}",
                esc(entry.get("title", "")),
                "המסחר המוקדם (pre-market) בניו יורק נפתח ב-04:00, והמסחר הרגיל ב-09:30 שעון ניו יורק.",
                f'<a href="{html.escape(entry.get("link", ""), quote=True)}">ההודעה המקורית</a>',
            ]
            await self.reply("\n".join(line for line in lines if line))

    def catalysts_text(self) -> str:
        today = us_eastern_now().date()
        upcoming = sorted((e for e in self.state.catalysts.values() if e.get("date", "") >= today.isoformat()),
                          key=lambda e: e["date"])
        if not upcoming:
            return "📅 אין כרגע קטליזטורים צפויים."
        lines = ["📅 <b>קטליזטורים צפויים</b>"]
        for e in upcoming:
            day = dt.date.fromisoformat(e["date"])
            lines.append(f"• {hebrew_date(day)} ({days_until_he(day, today)}) · <b>{esc(e['ticker'])}</b> · "
                         f"{esc(e['label'])}")
        return "\n".join(lines)

    # ----- guru portfolios (13F) -------------------------------------------

    async def check_gurus(self) -> None:
        """Every 30 minutes: has one of the tracked investors filed a new quarterly 13F?"""
        if not self.cfg.guru_alerts or not self.chat_id:
            return
        now = time.time()
        if now - self.state.guru_checked < GURU_CHECK_SECONDS:
            return
        self.state.guru_checked = now
        self.state.dirty = True
        for guru in self.cfg.gurus:
            try:
                await self.check_guru(guru)
            except SecBlocked:
                return
            except Exception as exc:  # noqa: BLE001
                log.warning("13F check for %s failed: %s", guru[1], describe_error(exc))

    async def check_guru(self, guru: tuple[int, str, str]) -> None:
        cik = guru[0]
        resp = await self.fetcher.get(SEC_SUBMISSIONS_URL.format(cik=cik))
        assert resp is not None
        rec = resp.json().get("filings", {}).get("recent", {})
        filings = [{k: rec[k][i] for k in ("accessionNumber", "filingDate", "reportDate")}
                   for i, form in enumerate(rec.get("form", [])) if form == "13F-HR"]
        if not filings:
            return
        latest = filings[0]
        known = self.state.gurus.get(str(cik))
        if known == latest["accessionNumber"]:
            return
        prev = next((f for f in filings[1:] if f["reportDate"] < latest["reportDate"]), None)
        cur = await self.load_13f(cik, latest["accessionNumber"])
        before = await self.load_13f(cik, prev["accessionNumber"]) if prev else {}
        changes = diff_13f(before, cur)
        cusips = {h.cusip for rows in changes.values() for p, h, _ in rows[:GURU_LIST_MAX] for h in (p, h) if h}
        tickers = await self.cusip_tickers(cusips, cur, before)
        await self.reply(format_guru_changes(guru, latest, cur, changes, tickers, first_report=known is None))
        self.state.gurus[str(cik)] = latest["accessionNumber"]
        self.state.dirty = True
        log.info("13F %s %s: %s", guru[1], latest["reportDate"], {k: len(v) for k, v in changes.items()})

    async def load_13f(self, cik: int, acc: str) -> dict[str, Holding]:
        resp = await self.fetcher.get(SEC_FILING_INDEX_JSON.format(cik=cik, acc_nd=acc.replace("-", "")))
        assert resp is not None
        items = resp.json().get("directory", {}).get("item", [])
        xmls = [i["name"] for i in items if i.get("name", "").lower().endswith(".xml")
                and i["name"].lower() != "primary_doc.xml"]
        if not xmls:
            return {}
        name = next((x for x in xmls if "info" in x.lower()), xmls[0])
        doc = await self.fetcher.get(f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-', '')}/{name}")
        assert doc is not None
        return parse_13f_table(doc.text)

    async def cusip_tickers(self, cusips: set[str], *tables: dict[str, Holding]) -> dict[str, str]:
        """CUSIP -> ticker: cached, else OpenFIGI (free, 10 per request), else a company-name match."""
        todo = sorted(c for c in cusips if c not in self.state.cusip_tickers)
        for i in range(0, len(todo), 10):
            batch = todo[i:i + 10]
            try:
                resp = await self.client.post(OPENFIGI_URL, timeout=15, json=[
                    {"idType": "ID_CUSIP", "idValue": c, "exchCode": "US"} for c in batch])
                resp.raise_for_status()
                for c, res in zip(batch, resp.json()):
                    data = res.get("data") or []
                    self.state.cusip_tickers[c] = normalize_ticker(data[0]["ticker"]) if data else ""
            except Exception as exc:  # noqa: BLE001
                log.info("OpenFIGI lookup failed: %s", describe_error(exc))
                break
        names = {h.cusip: h.name for t in tables for h in t.values()}
        out = {}
        for c in cusips:
            t = self.state.cusip_tickers.get(c) or self.tickers.by_name.get(normalize_company(names.get(c, "")), "")
            if t:
                out[c] = t
        self.state.dirty = True
        return out

    def gurus_text(self) -> str:
        lines = ["🧭 <b>משקיעי-על במעקב</b> (דיווחי 13F רבעוניים ל-SEC)"]
        for cik, name, fund in self.cfg.gurus:
            seen = "✅" if str(cik) in self.state.gurus else "⏳"
            lines.append(f"{seen} {esc(name)}" + (f" · {esc(fund)}" if fund else ""))
        lines.append(f'מקור: <a href="{html.escape(GURU_PAGE_URL, quote=True)}">GuruFocus</a> · נבדק כל 30 דקות')
        return "\n".join(lines)

    # ----- gainers study --------------------------------------------------

    async def check_gainers_study(self) -> None:
        """Once per trading day after the evening session: study the day's biggest gainers."""
        now = us_eastern_now()
        today = now.date()
        if (now.weekday() >= 5 or (now.hour, now.minute) < GAINERS_REPORT_AT
                or self.state.gainers_day == today.isoformat() or not self.chat_id):
            return
        self.state.gainers_day = today.isoformat()
        self.state.dirty = True
        day_start = dt.datetime.combine(today, dt.time(4, 0), tzinfo=eastern_tz()).timestamp() - 12 * 3600
        if not self.state.news_log_since or self.state.news_log_since > day_start:
            log.info("Gainers study: news log started mid-day, first study tomorrow")
            return
        candidates = await self.todays_movers(today)
        if not candidates:
            log.info("Gainers study: no stock up %s%%+ today", GAINERS_MIN_PCT)
            return
        studied = []
        for g in candidates[:GAINERS_TOP * 2]:
            if len(studied) >= GAINERS_TOP:
                break
            try:
                row = await self.study_gainer(g, today)
            except Exception as exc:  # noqa: BLE001
                log.info("Gainers study for %s failed: %s", g["ticker"], describe_error(exc))
                continue
            if row and row["volume"] >= GAINERS_MIN_VOLUME:
                studied.append(row)
            await asyncio.sleep(0.3)
        if not studied:
            return
        self.state.gainers_log.extend(studied)
        self.state.dirty = True
        await self.send_long(self.gainers_text(studied, today))
        days = {e["date"] for e in self.state.gainers_log}
        if len(days) >= LEARNING_READY_DAYS and not self.state.learning_ready_sent:
            self.state.learning_ready_sent = True
            await self.send_long(
                f"✅ <b>אני מוכן: סיימתי סבב למידה ראשון</b> ({len(days)} ימי מסחר)\n"
                "למדתי מה מקפיץ מניות, מאיזה מקור החדשות מגיעות ראשונות, וכמה מוקדם הבוט תופס אותן. "
                "הלמידה ממשיכה בכל יום, וההתראות מתעדכנות לפי מה שנמדד.\n\n"
                + learning_summary(self.state.gainers_log))

    async def todays_movers(self, today: dt.date, min_pct: float = GAINERS_MIN_PCT) -> list[dict[str, Any]]:
        """Every listed common share's change today (Yahoo spark, 20 symbols per request)."""
        universe = research_universe(self.tickers) if self.tickers.loaded else []
        moves: dict[str, tuple[float, float, float]] = {}
        for i in range(0, len(universe), SPARK_BATCH):
            batch = ",".join(universe[i:i + SPARK_BATCH])
            try:
                resp = await self.client.get(YAHOO_SPARK_URL.format(symbols=batch, range="5d"),
                                             headers=RESEARCH_HEADERS, timeout=20)
                if resp.status_code == 200:
                    moves.update(spark_moves(resp.json(), today))
                elif resp.status_code == 429:
                    await asyncio.sleep(10)
            except Exception as exc:  # noqa: BLE001
                log.info("Spark batch failed: %s", describe_error(exc))
            await asyncio.sleep(0.25)
        log.info("Gainers study: %d symbols priced, %d up %s%%+", len(moves),
                 sum(1 for m in moves.values() if m[0] >= GAINERS_MIN_PCT), GAINERS_MIN_PCT)
        return sorted(({"ticker": t, "pct": pct, "price": close, "prev": prev}
                       for t, (pct, close, prev) in moves.items()
                       if pct >= min_pct and close >= GAINERS_MIN_PRICE), key=lambda g: -g["pct"])

    async def study_gainer(self, g: dict[str, Any], today: dt.date) -> dict[str, Any] | None:
        sym = g["ticker"]
        tz = eastern_tz()
        day0 = dt.datetime.combine(today, dt.time(4, 0), tzinfo=tz).timestamp()
        resp = await self.client.get(YAHOO_CHART_URL.format(symbol=sym, p1=int(day0), p2=int(day0 + 16 * 3600),
                                                            interval=1), headers=RESEARCH_HEADERS, timeout=20)
        bars = parse_yahoo_chart(resp.json()) if resp.status_code == 200 else []
        prof = move_profile([(b[0], b[2]) for b in bars], g["prev"])
        if bars and (prof.get("peak_pct") or 0) < SPLIT_ARTIFACT_PCT:
            log.info("Gainers study: %s +%.0f%% is not a real move (reverse split?)", sym, g["pct"])
            return None
        start = prof.get("start")
        anchor = start or dt.datetime.combine(today, dt.time(16, 0), tzinfo=tz).timestamp()
        window = (anchor - 24 * 3600, anchor + 15 * 60)
        ours = [{**n, "seen": True} for n in self.state.news_log.get(sym, [])
                if window[0] <= (n.get("pub") or n["t"]) <= window[1]]
        theirs: list[dict[str, Any]] = []
        try:
            nr = await self.client.get(YAHOO_NEWS_URL.format(symbol=sym, count=15), headers=RESEARCH_HEADERS, timeout=20)
            theirs = [n for n in (yahoo_news_items(nr.json(), sym) if nr.status_code == 200 else [])
                      if window[0] <= n["pub"] <= window[1]]
        except Exception as exc:  # noqa: BLE001
            log.info("Yahoo news for %s unavailable: %s", sym, describe_error(exc))
        wires = [n for n in ours if not n["src"].startswith("SEC")]
        first_ours = min(wires or ours, key=lambda n: n.get("pub") or n["t"]) if ours else None
        first_any = min(theirs, key=lambda n: n["pub"]) if theirs else None
        # The earliest news anywhere; the bot "saw" it if the item is in its own log.
        first = first_ours
        if first_any and (not first_ours or first_any["pub"] < (first_ours.get("pub") or first_ours["t"]) - 60):
            first = first_any
        news_t = (first.get("pub") or first.get("t")) if first else None
        seen = bool(first and first.get("seen"))
        title = first["title"] if first and not first["src"].startswith("SEC") else ""
        alerts = [a for a in self.state.alert_log if a.get("ticker") == sym
                  and anchor - 24 * 3600 <= a["t"] <= anchor + 12 * 3600]
        alert_t = min(a["t"] for a in alerts) if alerts else None
        return {
            **g, "date": today.isoformat(), "volume": sum(b[5] for b in bars), "start": start,
            "peak_pct": prof.get("peak_pct"), "peak_t": prof.get("peak_t"),
            "news_src": first["src"] if first else "", "news_t": news_t, "seen": seen,
            "missing_source": bool(first) and not seen and not is_our_source(first["src"]),
            "title": title, "cat": classify_catalyst(title),
            "lead_min": round((start - news_t) / 60) if start and news_t else None,
            "bot_lead_min": round((start - alert_t) / 60) if start and alert_t else (0 if alert_t else None),
            "rule_score": rule_score(title).score if title else None,
        }

    def gainers_text(self, studied: list[dict[str, Any]], today: dt.date) -> str:
        tz = eastern_tz()
        hm = lambda t: dt.datetime.fromtimestamp(t, tz).strftime("%H:%M") if t else "—"  # noqa: E731
        lines = [f"🔥 <b>המזנקות של היום ולמה</b> ({today:%d.%m}, עלייה של {GAINERS_MIN_PCT:.0f}%+, שעון ניו יורק)"]
        for e in studied:
            head = f"\n<b>{esc(e['ticker'])}</b> {e['pct']:+.0f}%"
            if e.get("peak_pct") is not None:
                head += f" (שיא {e['peak_pct']:+.0f}% ב-{hm(e['peak_t'])})"
            if e.get("start"):
                head += f" · הזינוק התחיל {hm(e['start'])}"
            lines.append(head)
            if e.get("news_src") and not e.get("seen"):
                lead = e.get("lead_min")
                when = f" ({lead} דק' לפני הזינוק)" if lead is not None and lead >= 0 else ""
                why = "מקור שהבוט לא קורא" if e.get("missing_source") else "פורסם שם לפני שהבוט ראה"
                lines.append(f"📰 {esc(e['cat'])} · {esc(e['news_src'])} {hm(e['news_t'])}{when} · {why}")
                if e.get("title"):
                    lines.append(f"   {esc(e['title'][:110])}")
                if e.get("bot_lead_min") is None:
                    lines.append("🤖 הבוט לא התריע ❌")
            elif e.get("news_src"):
                lead = e.get("lead_min")
                when = (f"{lead} דק' לפני הזינוק" if lead is not None and lead >= 0
                        else f"{-lead} דק' אחרי שהזינוק התחיל" if lead is not None else "")
                lines.append(f"📰 {esc(e['cat'])} · {esc(e['news_src'])} {hm(e['news_t'])}" + (f" ({when})" if when else ""))
                if e.get("title"):
                    lines.append(f"   {esc(e['title'][:110])}")
                if e.get("bot_lead_min") is not None:
                    bl = e["bot_lead_min"]
                    lines.append("🤖 הבוט התריע " + (f"{bl} דק' לפני הזינוק ✅" if bl > 0 else
                                                    "בתחילת הזינוק ✅" if bl == 0 else f"{-bl} דק' אחרי שהתחיל ⚠️"))
                else:
                    lines.append(f"🤖 הבוט לא התריע ❌ (ציון לפי הכללים: {e.get('rule_score')})")
            else:
                lines.append("⚪ " + NO_NEWS + " (מומנטום / פמפום / תנועת סקטור)")
        lines.append("\n/learn לסיכום מה למדתי עד עכשיו")
        return "\n".join(lines)

    async def send_long(self, text: str) -> None:
        """Telegram caps a message at 4096 characters: split on line breaks."""
        chunk: list[str] = []
        for line in text.split("\n"):
            if chunk and sum(len(x) + 1 for x in chunk) + len(line) > 3800:
                await self.reply("\n".join(chunk))
                chunk = []
            chunk.append(line)
        if chunk:
            await self.reply("\n".join(chunk))

    # ----- breakout in progress (momentum) ---------------------------------

    async def momentum_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await self.check_momentum()
            except Exception:  # noqa: BLE001
                log.exception("Momentum check failed")
            await self._sleep(60)

    def momentum_due(self, now: float) -> list[str]:
        """Tickers with wire news in the last hours, checked every 2 minutes for 3 hours after the news,
        then every 10 minutes."""
        due = []
        for ticker, items in self.state.news_log.items():
            wires = [i for i in items if not i["src"].startswith("SEC") and now - i["t"] <= MOMENTUM_NEWS_HOURS * 3600]
            if not wires or ticker in self.state.momentum.get("tickers", []):
                continue
            age = now - max(i["t"] for i in wires)
            interval = 120 if age < 3 * 3600 else 600
            if now - self.momentum_checked.get(ticker, 0) >= interval:
                due.append(ticker)
        return due

    async def check_momentum(self) -> None:
        et = us_eastern_now()
        if et.weekday() >= 5 or not (4 <= et.hour < 20) or not self.chat_id:
            return
        if self.state.momentum.get("date") != et.date().isoformat():
            self.state.momentum = {"date": et.date().isoformat(), "tickers": []}
        now = time.time()
        due = self.momentum_due(now)[:MOMENTUM_MAX_CHECKS]
        if session_of(now) == "regular" and now - self.momentum_scan_at >= MOMENTUM_SCAN_SECONDS:
            self.momentum_scan_at = now
            movers = await self.todays_movers(et.date(), min_pct=MOMENTUM_PCT)
            due += [m["ticker"] for m in movers if m["ticker"] not in due
                    and m["ticker"] not in self.state.momentum["tickers"]][:30]
        for ticker in due:
            self.momentum_checked[ticker] = now
            await self.check_breakout(ticker, et.date())
            await asyncio.sleep(0.3)

    async def check_breakout(self, ticker: str, today: dt.date) -> None:
        day0 = dt.datetime.combine(today, dt.time(4, 0), tzinfo=eastern_tz()).timestamp()
        try:
            resp = await self.client.get(YAHOO_CHART_URL.format(symbol=ticker, p1=int(day0), p2=int(time.time()),
                                                                interval=1), headers=RESEARCH_HEADERS, timeout=10)
            if resp.status_code == 429:
                log.warning("Yahoo rate limit during the momentum scan")
                await asyncio.sleep(30)
                return
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            log.info("Momentum: chart for %s unavailable: %s", ticker, describe_error(exc))
            return
        bars = parse_yahoo_chart(data)
        meta = ((data.get("chart") or {}).get("result") or [{}])[0].get("meta") or {}
        prev = meta.get("chartPreviousClose") or meta.get("previousClose")
        if not bars or not prev:
            return
        price = bars[-1][4]
        pct = (price / prev - 1) * 100
        dollars = sum(b[4] * b[5] for b in bars)
        if pct < MOMENTUM_PCT or dollars < MOMENTUM_DOLLAR_VOLUME or ticker in self.state.momentum["tickers"]:
            return
        start = next((b[0] for b in bars if b[2] >= prev * (1 + MOVE_START_PCT)), bars[-1][0])
        self.state.momentum["tickers"].append(ticker)
        self.state.dirty = True
        await self.reply(self.breakout_text(ticker, pct, price, dollars, start, await self.recent_headlines(ticker)))
        log.info("BREAKOUT %s +%.0f%% ($%.0f traded)", ticker, pct, dollars)

    async def recent_headlines(self, ticker: str) -> list[dict[str, Any]]:
        """Yahoo's headlines about the ticker from the last 3 days, oldest first: the news the bot's own
        sources do not carry (Reuters, Dow Jones, foreign exchanges, Business Wire categories it skips)."""
        try:
            resp = await self.client.get(YAHOO_NEWS_URL.format(symbol=quote(ticker), count=10),
                                         headers=RESEARCH_HEADERS, timeout=6)
            items = yahoo_news_items(resp.json(), ticker)
        except Exception as exc:  # noqa: BLE001
            log.info("Momentum: news for %s unavailable: %s", ticker, describe_error(exc))
            return []
        return sorted((n for n in items if time.time() - n["pub"] <= 72 * 3600 and n["title"]), key=lambda n: n["pub"])

    def breakout_text(self, ticker: str, pct: float, price: float, dollars: float, start: float,
                      web_news: list[dict[str, Any]] | None = None) -> str:
        tz = eastern_tz()
        hm = lambda t: dt.datetime.fromtimestamp(t, tz).strftime("%H:%M")  # noqa: E731
        found = self.tickers.lookup(ticker)
        head = f"<b>{esc(ticker)}</b>" + (f" | {esc(found[1])}" if found else "")
        lines = ["🚀 <b>זינוק בתהליך</b> (המניה כבר עולה, זו לא תחזית)", head,
                 f"{pct:+.0f}% · ${price:.2f} · מחזור ${dollars / 1e6:.1f}M · עלתה מעל 10% ב-{hm(start)} (שעון ניו יורק)"]
        news = sorted((n for n in self.state.news_log.get(ticker, []) if time.time() - n["t"] <= 24 * 3600),
                      key=lambda n: n.get("pub") or n["t"])
        wires = [n for n in news if not n["src"].startswith("SEC")]
        if wires:
            n = wires[-1]
            score = rule_score(n["title"], found[1] if found else "", title=n["title"]).score
            lines.append(f"📰 {esc(n['src'])} {hm(n.get('pub') or n['t'])} · {esc(classify_catalyst(n['title']))} "
                         f"(ציון {score}):")
            lines.append(f"   {esc(n['title'][:120])}")
        elif web_news:
            n = web_news[-1]
            age = time.time() - n["pub"]
            when = hm(n["pub"]) if age < 20 * 3600 else f"לפני {age / 86400:.0f} ימים: המשך של תנועה קודמת"
            lines.append(f"📰 {esc(n['src'] or 'Yahoo')} {when} · {esc(classify_catalyst(n['title']))}:")
            lines.append(f"   {esc(n['title'][:120])}")
            if news:
                lines.append(f"   + דיווח ל-SEC: {esc(news[-1]['title'])}")
        elif news:
            lines.append(f"📰 דיווח ל-SEC: {esc(news[-1]['title'])}")
        else:
            lines.append("⚪ אין שום ידיעה ב-3 הימים האחרונים (גם לא ב-Yahoo): כנראה פמפום, שמועה או תנועת סקטור")
        lines.append(f'<a href="https://finance.yahoo.com/quote/{quote(ticker)}">גרף ב-Yahoo</a>')
        return "\n".join(lines)

    # ----- performance report ----------------------------------------------

    async def check_performance_report(self) -> None:
        """Once per trading day, after the evening session (20:10 New York): measure every alert
        not measured yet as if it had been bought 3 minutes after it went out, and report."""
        now = us_eastern_now()
        today = now.date().isoformat()
        if now.weekday() >= 5 or (now.hour, now.minute) < PERF_REPORT_AT or self.state.perf_day == today:
            return
        self.state.perf_day = today
        self.state.dirty = True
        fresh = await self.measure_alerts()
        if not fresh:
            return
        lines = ["📈 <b>דוח ביצועים יומי</b>: מה היה קורה אם היית קונה 3 דקות אחרי כל התראה"]
        for e in fresh:
            r = e["r"]
            head = f"<b>{esc(e['ticker'])}</b>"
            if e.get("score") is not None:
                head += f" (+{e['score']})"
            if e.get("pump", "none") in ("high", "medium"):
                head += " " + PUMP_HE[e["pump"]][:1]
            if not r.get("tradable"):
                lines.append(f"• {head}: השוק היה סגור. קנייה בפתיחה ב-${r['entry']:.2f}, סגירה {_pct(r['r_close'])}")
                continue
            lines.append(f"• {head}: כניסה ${r['entry']:.2f} אחרי {fmt_duration(r['entry_delay_s'])} · "
                         f"5 דק' {_pct(r['r_5m'])} · 30 דק' {_pct(r['r_30m'])} · סגירה {_pct(r['r_close'])}")
        chunk: list[str] = []
        for line in lines:  # Telegram caps a message at 4096 characters
            if chunk and sum(len(x) + 1 for x in chunk) + len(line) > 3500:
                await self.reply("\n".join(chunk))
                chunk = []
            chunk.append(line)
        await self.reply("\n".join(chunk))
        await self.reply(perf_summary_text(self.state.alert_log) + "\nℹ️ מדידה על נתוני עבר, לא ייעוץ השקעות.")

    async def measure_alerts(self) -> list[dict[str, Any]]:
        """Fetch 1-minute bars (Yahoo keeps them 30 days) for alerts whose trading day is over."""
        now = time.time()
        today = us_eastern_now().date()
        fresh = []
        for e in self.state.alert_log:
            if "r" in e or e.get("skip") or now - e["t"] < ENTRY_DELAY + 300:
                continue
            if now - e["t"] > 28 * 86400:
                e["skip"] = "ישן מדי לנתוני דקה"
                continue
            url = YAHOO_CHART_URL.format(symbol=e["ticker"], p1=int(e["t"] - 7200),
                                         p2=int(min(now, e["t"] + 4 * 86400)), interval=1)
            try:
                resp = await self.client.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
                resp.raise_for_status()
                sim = simulate_trade(parse_yahoo_chart(resp.json()), e["t"])
            except Exception as exc:  # noqa: BLE001
                log.info("Performance: prices for %s unavailable: %s", e["ticker"], describe_error(exc))
                sim = None
            if sim is None:
                if now - e["t"] > 3 * 86400:
                    e["skip"] = "אין נתוני מחיר"
                continue
            if dt.datetime.fromtimestamp(sim["entry_ts"], eastern_tz()).date() >= today \
                    and (us_eastern_now().hour, us_eastern_now().minute) < PERF_REPORT_AT:
                continue  # the entry day is not over yet
            e["r"] = {k: sim[k] for k in ("entry", "entry_delay_s", "tradable", "session", "pre_move",
                                          "r_5m", "r_30m", "r_60m", "r_close", "tp2_sl2")}
            fresh.append(e)
        if fresh:
            self.state.dirty = True
        return fresh

    # ----- Telegram commands ----------------------------------------------

    def help_text(self) -> str:
        engine = f"Claude ({self.cfg.anthropic_model})" if self.cfg.anthropic_key else "כללים"
        mode = "שוק מלא + רשימת מעקב" if self.cfg.marketwide else "רשימת מעקב בלבד"
        return (
            "📡 <b>Stock News Radar</b>\n"
            "סורק דיווחי SEC (8-K, 6-K) והודעות לעיתונות של מניות אמריקאיות, "
            "ושולח רק ידיעות חיוביות מאוד.\n\n"
            "<b>פקודות</b>\n"
            "/add NVDA OKLO — הוספה לרשימת המעקב\n"
            "/remove NVDA — הסרה מהרשימה\n"
            "/list — הצגת הרשימה\n"
            "/catalysts — קטליזטורים צפויים (תוצאות ניסויים, החלטות FDA)\n"
            "/perf — מה היה קורה אם היית קונה 3 דקות אחרי כל התראה\n"
            "/gurus — משקיעי-העל שבמעקב (קניות ומכירות מדיווחי 13F)\n"
            "/learn — מה למדתי על המניות שמזנקות (סוג חדשות, מקור, תזמון)\n"
            "🚀 התראת \"זינוק בתהליך\" נשלחת אוטומטית כשמניה עולה 15%+ עם מחזור אמיתי\n"
            "/status — מצב המקורות והמונים\n"
            "/test — התראת דוגמה\n"
            "/help — ההודעה הזו\n\n"
            f"⚙️ מנוע דירוג: {engine} · סף: {self.cfg.min_score} · מצב: {mode} · "
            f"מניות במעקב: {len(self.state.watchlist)}\n"
            "ℹ️ מידע בלבד, לא ייעוץ השקעות."
        )

    def status_text(self) -> str:
        now = time.time()
        lines = ["📊 <b>סטטוס</b>"]
        if self.once:
            lines.append("⏱ מצב --once · הנתונים של ההרצה הזו")
        else:
            lines.append(f"⏱ זמן פעילות: {fmt_duration(now - self.started)}")
        lines.append(f"🔎 ידיעות שנבדקו: {self.stats['checked']} · מועמדים: {self.stats['candidates']}"
                     f" · התראות: {self.stats['alerts']}")
        if self.cfg.anthropic_key:
            lines.append(f"🤖 קריאות AI: {self.stats['ai_calls']} (שגיאות: {self.stats['ai_errors']})")
        else:
            lines.append("🤖 AI כבוי — דירוג לפי כללים")
        lines.append("")
        lines.append("<b>מקורות</b>")
        names = [f"SEC {f}" for f in self.cfg.edgar_forms] + \
            [wire_source_name(u) for u in self.cfg.wire_feeds] + ["SEC Tickers"]
        for name in names:
            s = self.state.sources.get(name)
            if not s:
                lines.append(f"⏳ {esc(name)} — עוד לא נדגם")
                continue
            last = f"עודכן לפני {fmt_duration(now - s['last_ok'])}" if s.get("last_ok") else "לא עודכן"
            if s.get("ok"):
                line = f"✅ {esc(name)} — {last}"
            else:
                line = f"❌ {esc(name)} — {esc(s.get('error', ''))} ({last})"
            if s.get("overflows"):
                line += (f"\n   ⚠️ ייתכן שפוספסו ידיעות {s['overflows']} פעמים "
                         f"(אחרון לפני {fmt_duration(now - s['last_overflow'])})")
            lines.append(line)
        blocked = self.fetcher.sec_blocked_until - now
        if blocked > 0:
            lines.append(f"⛔ SEC חסום, חידוש בעוד {fmt_duration(blocked)}")
        return "\n".join(lines)

    async def reply(self, text: str) -> None:
        if self.chat_id:
            try:
                await self.tg.send(self.chat_id, text)
            except Exception as exc:  # noqa: BLE001
                log.error("Telegram reply failed: %s", exc)

    async def handle_update(self, update: dict[str, Any]) -> None:
        msg = update.get("message") or update.get("edited_message")
        if not msg:
            return
        chat = msg.get("chat", {})
        chat_id = str(chat.get("id", ""))
        text = (msg.get("text") or "").strip()
        if not self.chat_id:
            if chat.get("type") == "private" and not self.once:
                self.chat_id = chat_id
                try:
                    save_env_var(self.cfg.env_file, "TELEGRAM_CHAT_ID", chat_id)
                    saved = f"ונשמר ב-{self.cfg.env_file}"
                except OSError as exc:
                    log.error("Could not save chat id to %s: %s", self.cfg.env_file, exc)
                    saved = "(לא ניתן היה לשמור ל-.env — הגדר TELEGRAM_CHAT_ID ידנית)"
                log.info("Locked on chat id %s", chat_id)
                await self.reply(f"🔒 הבוט ננעל על הצ'אט הזה (chat id {chat_id}) {saved}.\n\n"
                                 + self.help_text())
            return
        if chat_id != self.chat_id or not text.startswith("/"):
            return
        parts = text.split()
        cmd = parts[0].split("@")[0].lower()
        args = parts[1:]
        if cmd in ("/start", "/help"):
            await self.reply(self.help_text())
        elif cmd == "/add":
            await self.reply(self.cmd_add(args))
        elif cmd == "/remove":
            await self.reply(self.cmd_remove(args))
        elif cmd in ("/catalysts", "/upcoming"):
            await self.reply(self.catalysts_text())
        elif cmd in ("/learn", "/gainers"):
            await self.send_long(learning_summary(self.state.gainers_log))
        elif cmd in ("/gurus", "/guru"):
            await self.reply(self.gurus_text())
        elif cmd in ("/perf", "/performance"):
            await self.reply(perf_summary_text(self.state.alert_log))
        elif cmd == "/list":
            wl = self.state.watchlist
            await self.reply("📋 רשימת מעקב: " + (", ".join(wl) if wl else "ריקה"))
        elif cmd == "/status":
            if self.once:
                self.pending_status = True
            else:
                await self.reply(self.status_text())
        elif cmd == "/test":
            await self.reply(sample_alert())
        else:
            await self.reply("פקודה לא מוכרת. /help לרשימת הפקודות.")

    def cmd_add(self, args: list[str]) -> str:
        if not args:
            return "שימוש: /add NVDA OKLO"
        added, unknown, bad = [], [], []
        for raw in args:
            t = normalize_ticker(raw.strip(","))
            if not re.fullmatch(r"[A-Z][A-Z0-9\-]{0,9}", t):
                bad.append(raw)
                continue
            if t not in self.state.watchlist:
                self.state.watchlist.append(t)
                added.append(t)
            if self.tickers.loaded and not self.tickers.lookup(t):
                unknown.append(t)
        self.state.dirty = True
        parts = [f"✅ נוסף: {', '.join(added)}" if added else "לא נוספו מניות חדשות"]
        if unknown:
            parts.append(f"⚠️ לא נמצאו ברשימת החברות הנסחרות בארה\"ב: {', '.join(unknown)}")
        if bad:
            parts.append(f"❌ לא תקין: {esc(' '.join(bad))}")
        parts.append("📋 " + ", ".join(self.state.watchlist))
        return "\n".join(parts)

    def cmd_remove(self, args: list[str]) -> str:
        if not args:
            return "שימוש: /remove NVDA"
        removed = []
        for raw in args:
            t = normalize_ticker(raw.strip(","))
            if t in self.state.watchlist:
                self.state.watchlist.remove(t)
                removed.append(t)
        self.state.dirty = True
        wl = ", ".join(self.state.watchlist) or "ריקה"
        return (f"🗑 הוסר: {', '.join(removed)}" if removed else "המניה לא ברשימה") + f"\n📋 {wl}"

    async def process_updates(self, timeout: int) -> None:
        payload: dict[str, Any] = {"timeout": timeout, "allowed_updates": ["message", "edited_message"]}
        if self.state.tg_offset:
            payload["offset"] = self.state.tg_offset
        updates = await self.tg.call("getUpdates", payload, timeout=timeout + 15)
        for upd in updates or []:
            self.state.tg_offset = int(upd["update_id"]) + 1
            self.state.dirty = True
            try:
                await self.handle_update(upd)
            except Exception:  # noqa: BLE001
                log.exception("Handling update failed")

    # ----- loops -----------------------------------------------------------

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def edgar_loop(self) -> None:
        while not self.stop_event.is_set():
            started = time.monotonic()
            for form in self.cfg.edgar_forms:
                await self.poll_edgar(form)
            await self._sleep(max(0.0, self.cfg.edgar_poll - (time.monotonic() - started)))

    async def wire_loop(self) -> None:
        # Category feeds only back up the main feeds when these overflow between polls, which
        # cannot happen at this pace; polling them every few cycles keeps the request rate low.
        main = [u for u in self.cfg.wire_feeds if " · " not in wire_source_name(u)]
        extra = [u for u in self.cfg.wire_feeds if u not in main]
        cycle = 0
        while not self.stop_event.is_set():
            started = time.monotonic()
            feeds = main + (extra if cycle % CATEGORY_FEED_EVERY == 0 else [])
            await asyncio.gather(*(self.poll_wire(u) for u in feeds))
            cycle += 1
            await self._sleep(max(0.0, self.cfg.wire_poll - (time.monotonic() - started)))

    async def command_loop(self) -> None:
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                await self.process_updates(timeout=25)
                if time.monotonic() - started < 0.5:
                    await self._sleep(0.5)  # never spin if long polling returns instantly
            except TelegramError as exc:
                log.warning("getUpdates failed: %s", exc)
                if exc.status == 401:
                    log.error("Telegram token rejected (401) — check TELEGRAM_BOT_TOKEN")
                await self._sleep(30 if exc.status in (401, 409) else 5)
            except Exception as exc:  # noqa: BLE001
                log.warning("getUpdates failed: %s", exc)
                await self._sleep(5)

    async def catalyst_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await self.check_catalyst_reminders()
                await self.check_performance_report()
                await self.check_gurus()
                await self.check_gainers_study()
            except Exception:  # noqa: BLE001
                log.exception("Catalyst reminders / performance report failed")
            await self._sleep(60)

    async def ticker_refresh_loop(self) -> None:
        while not self.stop_event.is_set():
            fresh = self.tickers.loaded and time.time() - self.tickers.loaded_at < TICKER_REFRESH_SECONDS
            if not fresh:
                await self.refresh_tickers()
            await self._sleep(TICKER_REFRESH_SECONDS if self.tickers.loaded else 300)

    async def save_loop(self) -> None:
        while not self.stop_event.is_set():
            await self._sleep(5)
            self.save_state()

    def save_state(self) -> None:
        if self.state.dirty:
            try:
                self.state.save()
            except OSError as exc:
                log.error("Saving state failed: %s", exc)

    async def run(self, duration: float | None = None) -> None:
        """Continuous polling. With a duration (GitHub Actions) it stops by itself after that many
        seconds and stays silent on start, since the next run takes over right away."""
        await self.refresh_tickers()
        self.state.dirty = True
        self.save_state()
        if duration:
            duration = quiet_stop(time.time(), duration)
            asyncio.get_running_loop().call_later(duration, self.stop_event.set)
            log.info("Continuous run for %d minutes", duration // 60)
        elif self.chat_id:
            await self.reply("🟢 <b>Stock News Radar פעיל</b>\n" + self.help_text())
        else:
            log.warning("TELEGRAM_CHAT_ID is empty — open the bot in Telegram and press Start")
        loops = [self.edgar_loop(), self.wire_loop(), self.command_loop(),
                 self.ticker_refresh_loop(), self.save_loop(), self.catalyst_loop(), self.momentum_loop()]
        tasks = [asyncio.create_task(c) for c in loops]
        try:
            await self.stop_event.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.drain(timeout=10)
            self.state.dirty = True
            self.save_state()

    async def run_demo(self, per_source: int = 4) -> int:
        """Score the newest REAL items in every feed (ignoring 'seen' and the threshold) and
        send the best one plus a summary to Telegram. State is not touched."""
        await self.refresh_tickers()
        cands: list[Candidate] = []
        for form in self.cfg.edgar_forms:
            try:
                resp = await self.fetcher.get(EDGAR_FEED_URL.format(form=quote(form)))
            except Exception as exc:  # noqa: BLE001
                log.warning("demo: SEC %s failed: %s", form, describe_error(exc))
                continue
            found = [c for f in parse_edgar_feed(resp.content) if f.form == form
                     for c in [self.edgar_candidate(f)] if c]
            cands += found[:per_source]
        for url in self.cfg.wire_feeds:
            try:
                resp = await self.fetch_wire_feed(url, conditional=False)
            except Exception as exc:  # noqa: BLE001
                log.warning("demo: %s failed: %s", wire_source_name(url), describe_error(exc))
                continue
            found = [c for it in parse_wire_feed(resp.content)
                     for c in [self.wire_candidate(it, wire_source_name(url))] if c]
            cands += found[:per_source]
        if not cands:
            await self.reply("🧪 הדגמה: לא נמצאו כרגע ידיעות עם טיקר אמריקאי בפידים.")
            return 1

        results = []
        for c in cands:
            score, reason, rejected, _ = await self.evaluate(c)
            results.append((c, score, reason, rejected))
            log.info("demo: %-6s %+d %s %s", c.ticker, score, f"[{rejected}]" if rejected else "",
                     c.title or c.items)

        ok = [r for r in results if not r[3]]
        best = max(ok or results, key=lambda r: r[1])
        c, score, reason, rejected = best
        passed = score >= self.cfg.min_score and not rejected
        header = ("🧪 <b>הדגמה עם ידיעה אמיתית</b> — "
                  + ("הייתה נשלחת כהתראה" if passed else
                     f"לא הייתה נשלחת (סף {self.cfg.min_score})") + "\n\n")
        await self.reply(header + format_alert(c, score, reason or (rejected or "")))

        lines = [f"📋 <b>נבדקו {len(results)} ידיעות אמיתיות מהפידים</b> (ציון · טיקר · מקור)"]
        for c2, sc, _, rej in sorted(results, key=lambda r: -r[1]):
            what = esc((c2.title or ", ".join(f"Item {i}" for i in c2.items) or c2.form)[:70])
            tag = f"❌ נפסל: {esc(rej)}" if rej else f"{sc:+d}"
            lines.append(f"{tag} · <b>{esc(c2.ticker or '—')}</b> · {esc(c2.source_label)} · {what}")
        lines.append(f"\nרק ציון {self.cfg.min_score}+ נשלח כהתראה אמיתית.")
        await self.reply("\n".join(lines))
        log.info("demo done: %d items, best %s %+d", len(results), c.ticker, score)
        return 0

    async def run_once(self) -> int:
        """One pass. Returns 1 when Telegram rejects the token (so CI shows red), else 0."""
        await self.refresh_tickers()
        try:
            await self.process_updates(timeout=0)
        except TelegramError as exc:
            if exc.status in (401, 404):
                log.error("Telegram rejected TELEGRAM_BOT_TOKEN (%s). %s", exc.status,
                          token_hint(self.cfg.telegram_token))
                return 1
            log.warning("Processing pending Telegram commands failed: %s", exc)
        except Exception as exc:  # noqa: BLE001
            log.warning("Processing pending Telegram commands failed: %s", exc)
        # All sources at once: a slow SEC feed must not hold back the press-release feeds.
        await asyncio.gather(*(self.poll_edgar(f) for f in self.cfg.edgar_forms),
                             *(self.poll_wire(u) for u in self.cfg.wire_feeds))
        await self.drain()
        await self.check_catalyst_reminders()
        await self.check_performance_report()
        await self.check_gurus()
        await self.check_gainers_study()
        if self.pending_status:
            await self.reply(self.status_text())
        self.state.dirty = True
        self.save_state()
        log.info("Once run done: checked=%d candidates=%d alerts=%d",
                 self.stats["checked"], self.stats["candidates"], self.stats["alerts"])
        for name, s in sorted(self.state.sources.items()):
            log.info("  %s %s%s", "✅" if s.get("ok") else "❌", name,
                     "" if s.get("ok") else f" — {s.get('error')}")
        return 0


# ---------------------------------------------------------------------------
# --test
# ---------------------------------------------------------------------------


async def detect_private_chat(tg: Telegram) -> str | None:
    """The chat id of the latest private chat that messaged the bot (last 24h), if any."""
    try:
        updates = await tg.call("getUpdates", {"timeout": 0})
    except TelegramError as exc:
        print(f"⚠️ getUpdates נכשל: {exc}")
        return None
    private = [
        str(u["message"]["chat"]["id"]) for u in updates or []
        if u.get("message", {}).get("chat", {}).get("type") == "private"
    ]
    return private[-1] if private else None


NO_CHAT_HELP = ("   פתח את הבוט בטלגרם, לחץ Start (או שלח לו הודעה כלשהי), והרץ שוב את הבדיקה.")


async def run_test(cfg: Config, client: httpx.AsyncClient) -> int:
    tg = Telegram(client, cfg.telegram_token)
    try:
        me = await tg.call("getMe")
    except TelegramError as exc:
        if exc.status in (401, 404):
            print(f"❌ טלגרם דחה את הטוקן ({exc.status}). {token_hint(cfg.telegram_token)}")
            print("   העתק מחדש את הטוקן מ-@BotFather ועדכן את TELEGRAM_BOT_TOKEN.")
        else:
            print(f"❌ שגיאה בחיבור לטלגרם: {exc}")
        return 1
    bot_name = f"@{me.get('username')}"
    print(f"✅ הטוקן תקין: {bot_name}")

    chat_id = cfg.chat_id
    if chat_id and chat_id == str(me.get("id")):
        print("❌ TELEGRAM_CHAT_ID הוא המספר של הבוט עצמו (המספר שבתחילת הטוקן), "
              "ולא ה-chat id שלך. מנסה לזהות את ה-chat id הנכון...")
        chat_id = ""

    if not chat_id:
        detected = await detect_private_chat(tg)
        if not detected:
            print(f"⚠️ לא נמצאה שיחה פרטית עם {bot_name}.")
            print(NO_CHAT_HELP)
            return 1
        # Configured but wrong, or running in CI (public logs): tell the user privately, don't save.
        if cfg.chat_id or os.getenv("GITHUB_ACTIONS") == "true":
            return await _send_detected(tg, detected, bot_name)
        chat_id = detected
        try:
            save_env_var(cfg.env_file, "TELEGRAM_CHAT_ID", chat_id)
            print(f"✅ זוהה chat id {chat_id} ונשמר ב-{cfg.env_file}")
        except OSError as exc:
            print(f"⚠️ זוהה chat id {chat_id} אבל השמירה ל-{cfg.env_file} נכשלה: {exc}")

    try:
        await tg.send(chat_id, sample_alert())
    except TelegramError as exc:
        if exc.status == 400:
            print(f"❌ טלגרם לא מכיר את הצ'אט שב-TELEGRAM_CHAT_ID ({exc.description}).")
            print(f"   או שעוד לא לחצת Start ב-{bot_name}, או שה-chat id שגוי. מנסה לזהות אוטומטית...")
            detected = await detect_private_chat(tg)
            if detected and detected != chat_id:
                return await _send_detected(tg, detected, bot_name)
            print(NO_CHAT_HELP)
        elif exc.status == 403:
            print(f"❌ הבוט חסום, או שעוד לא לחצת Start בצ'אט עם {bot_name} (403).")
        else:
            print(f"❌ שליחה נכשלה: {exc}")
        return 1
    print("✅ התראת דוגמה נשלחה לטלגרם")
    return 0


async def _send_detected(tg: Telegram, chat_id: str, bot_name: str) -> int:
    """Send the detected chat id to that chat privately (CI logs of a public repo are public)."""
    text = (f"🔑 זה ה-chat id שלך: <code>{esc(chat_id)}</code>\n"
            "עדכן אותו בסוד TELEGRAM_CHAT_ID ב-GitHub והרץ שוב את הבדיקה.\n\n" + sample_alert())
    try:
        await tg.send(chat_id, text)
    except TelegramError as exc:
        print(f"❌ גם השליחה לצ'אט שזוהה נכשלה: {exc}")
        return 1
    print(f"📨 נמצאה שיחה פרטית עם {bot_name}. שלחתי לך שם הודעה עם ה-chat id הנכון.")
    print("   עדכן את הסוד TELEGRAM_CHAT_ID לפי ההודעה בטלגרם, והרץ שוב את הבדיקה.")
    return 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(20.0, connect=10.0),
        follow_redirects=True,
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
    )


async def _main_async(args: argparse.Namespace, cfg: Config) -> int:
    async with make_client() as client:
        if args.test:
            return await run_test(cfg, client)
        radar = Radar(cfg, client, once=args.once or args.demo)
        if args.demo:
            return await radar.run_demo()
        if args.once:
            return await radar.run_once()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, radar.stop_event.set)
            except (NotImplementedError, RuntimeError):
                pass
        await radar.run(duration=args.run_for)
        return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stock News Radar — Telegram bot")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--test", action="store_true", help="check token, detect chat id, send a sample alert")
    group.add_argument("--once", action="store_true", help="single pass over all sources, then exit")
    group.add_argument("--demo", action="store_true",
                       help="score the newest real items and send the best one + a summary (no state)")
    group.add_argument("--run-for", type=float, metavar="SECONDS",
                       help="poll continuously for SECONDS, then exit (GitHub Actions)")
    args = parser.parse_args(argv)

    load_dotenv(Path(_env("ENV_FILE", ".env")))
    logging.basicConfig(
        level=_env("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    cfg = Config.from_env()

    missing = []
    if not cfg.telegram_token:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not args.test and not cfg.sec_user_agent:
        missing.append("SEC_USER_AGENT")
    if (args.once or args.demo or args.run_for) and not cfg.chat_id:
        missing.append("TELEGRAM_CHAT_ID (חובה במצב --once / --demo)")
    if missing:
        print("❌ חסרים משתני סביבה: " + ", ".join(missing))
        return 2
    try:
        return asyncio.run(_main_async(args, cfg))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
