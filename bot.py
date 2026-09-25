#!/usr/bin/env python3
"""Stock News Radar — personal Telegram bot for very positive US-stock news.

Listens to primary sources only (SEC EDGAR 8-K / 6-K live feeds and the
PR Newswire / GlobeNewswire RSS feeds), scores each item for positivity
(rules engine, or Claude when ANTHROPIC_API_KEY is set) and sends a Hebrew
alert to a single private Telegram chat when the score passes MIN_SCORE.

Usage:
    python bot.py            # run forever (server mode)
    python bot.py --once     # one pass over all sources, then exit (GitHub Actions)
    python bot.py --test     # verify token, detect/save chat id, send a sample alert
"""
from __future__ import annotations

import argparse
import asyncio
import calendar
import html
import json
import logging
import os
import re
import signal
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin, urlparse

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
DEFAULT_WIRE_FEEDS = (
    "https://www.prnewswire.com/rss/news-releases-list.rss,"
    "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/"
    "GlobeNewswire%20-%20News%20about%20Public%20Companies"
)
TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
DEFAULT_MODEL = "claude-haiku-4-5-20251001"

WIRE_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/128.0.0.0 Safari/537.36"
)
WIRE_HEADERS = {
    "Accept": "application/rss+xml, application/xml;q=0.9, text/html;q=0.8, */*;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
}
WIRE_ATTEMPTS = 2                # one retry on timeout / 5xx
TOKEN_RE = re.compile(r"^\d{5,}:[A-Za-z0-9_-]{30,}$")

SEC_BLOCK_SECONDS = 600          # wait 10 minutes after 403/429 from SEC
SEC_MIN_INTERVAL = 0.15          # <= ~7 requests/second, well under SEC's 10/s
TICKER_REFRESH_SECONDS = 24 * 3600
MAX_SEEN = 30_000
AI_CONCURRENCY = 5
AI_TEXT_CHARS = 6_000
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
    (3, "Breakthrough Therapy", re.compile(r"\bbreakthrough therapy\b", I)),
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
    edgar_forms: list[str] = field(default_factory=lambda: ["8-K", "6-K"])
    edgar_poll: float = 2.0
    wire_poll: float = 10.0
    wire_feeds: list[str] = field(default_factory=lambda: _split(DEFAULT_WIRE_FEEDS))
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
            edgar_forms=[f.upper() for f in _split(_env("EDGAR_FORMS", "8-K,6-K"))],
            edgar_poll=max(1.0, _env_float("EDGAR_POLL_SECONDS", 2.0)),
            wire_poll=max(2.0, _env_float("WIRE_POLL_SECONDS", 10.0)),
            wire_feeds=_split(_env("WIRE_FEEDS", DEFAULT_WIRE_FEEDS)),
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


def rule_score(text: str, company: str = "") -> RuleResult:
    score = 0
    labels: list[str] = []
    for points, label, pattern in POSITIVE_RULES:
        if pattern.search(text):
            score += points
            if label not in labels:
                labels.append(label)
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
    host = urlparse(url).netloc.lower()
    for needle, name in (("prnewswire", "PR Newswire"), ("globenewswire", "GlobeNewswire"),
                         ("businesswire", "Business Wire"), ("accessnewswire", "ACCESS Newswire")):
        if needle in host:
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
        return f"HTTP {exc.response.status_code}"
    if isinstance(exc, httpx.TimeoutException):
        return f"timeout ({type(exc).__name__})"
    return str(exc).split("\n")[0] or type(exc).__name__


class Fetcher:
    """GET with ETag / If-Modified-Since, SEC rate limiting and SEC block handling."""

    def __init__(self, client: httpx.AsyncClient, sec_user_agent: str):
        self.client = client
        self.sec_user_agent = sec_user_agent
        self.validators: dict[str, tuple[str | None, str | None]] = {}
        self.sec_blocked_until = 0.0
        self._sec_lock = asyncio.Lock()
        self._sec_last = 0.0

    @staticmethod
    def is_sec(url: str) -> bool:
        return urlparse(url).netloc.lower().endswith("sec.gov")

    async def get(self, url: str, conditional: bool = False) -> httpx.Response | None:
        """Returns the response, or None when a conditional request got 304."""
        sec = self.is_sec(url)
        headers = {"User-Agent": self.sec_user_agent} if sec else {"User-Agent": WIRE_USER_AGENT, **WIRE_HEADERS}
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
        resp = await self.client.get(url, headers=headers)
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


class TickerMap:
    def __init__(self) -> None:
        self.by_ticker: dict[str, tuple[int, str]] = {}
        self.by_cik: dict[int, list[str]] = {}
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

    def load(self, data: Any) -> None:
        self.by_ticker, self.by_cik = {}, {}
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
        return st

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
        data = {
            "watchlist": self.watchlist,
            "initialized": sorted(self.initialized),
            "last_alert": self.last_alert,
            "tg_offset": self.tg_offset,
            "sources": self.sources,
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


def format_alert(c: Candidate, score: int | None, reason: str, now: float | None = None) -> str:
    now = time.time() if now is None else now
    lines = [score_tag(score)]
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
        self.fetcher = Fetcher(client, cfg.sec_user_agent)
        self.tickers = TickerMap()
        self.tg = Telegram(client, cfg.telegram_token)
        self.chat_id = cfg.chat_id
        self.ai_sem = asyncio.Semaphore(AI_CONCURRENCY)
        self.tasks: set[asyncio.Task[Any]] = set()
        self.started = time.time()
        self.stats = {"checked": 0, "candidates": 0, "ai_calls": 0, "ai_errors": 0, "alerts": 0}
        self.stop_event = asyncio.Event()

    # ----- source status -------------------------------------------------

    def _source_ok(self, name: str) -> None:
        self.state.sources[name] = {"ok": True, "last_ok": time.time(), "error": ""}

    def _source_error(self, name: str, error: str) -> None:
        prev = self.state.sources.get(name, {})
        self.state.sources[name] = {"ok": False, "last_ok": prev.get("last_ok"), "error": error[:200]}

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
            resp = await self.fetcher.get(url, conditional=True)
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
            if cand is None:
                continue
            if dispatched >= self.cfg.max_per_cycle:
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

    async def poll_wire(self, url: str) -> None:
        name = wire_source_name(url)
        resp = None
        for attempt in range(1, WIRE_ATTEMPTS + 1):
            try:
                resp = await self.fetcher.get(url, conditional=True)
                break
            except Exception as exc:  # noqa: BLE001
                retryable = isinstance(exc, httpx.TransportError) or (
                    isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500)
                if retryable and attempt < WIRE_ATTEMPTS:
                    await asyncio.sleep(2)
                    continue
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
            if cand is None:
                continue
            if dispatched >= self.cfg.max_per_cycle:
                log.warning("%s: over MAX_ALERTS_PER_CYCLE, skipping %s", name, it.title)
                continue
            dispatched += 1
            self.spawn(self.process(cand))
        if first:
            self.state.initialized.add(init_key)
            self.state.dirty = True
            log.info("%s initialized: %d existing items marked as seen", name, len(items))

    def wire_candidate(self, it: WireItem, source_name: str) -> Candidate | None:
        if not it.tickers:
            return None  # no US exchange ticker in the release
        ticker = it.tickers[0]
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

    async def process(self, c: Candidate) -> None:
        self.stats["candidates"] += 1
        try:
            text = await self.fetch_text(c)
        except Exception as exc:  # noqa: BLE001
            log.warning("Fetching text for %s failed: %s", c.link, exc)
            text = c.summary
        body = strip_boilerplate(text)
        head = f"{c.title}\n{body}" if c.title else body
        score: int | None = None
        reason = ""
        if self.cfg.positive_only or not c.watch:
            neg = negative_hit(f"{c.title}\n{body[:NEGATIVE_CHARS]}")
            if neg:
                log.info("Rejected %s (%s): %s", c.ticker, neg, c.title or c.items)
                return
            rules = rule_score(f"{c.title}\n{body[:LEAD_CHARS]}", c.company)
            score, reason = rules.score, rules.reason_he
            if self.cfg.anthropic_key and (rules.score >= 1 or c.watch):
                ai = await self.claude_score(c, head)
                if ai is not None:
                    score = ai["score"]
                    reason = ai["reason_he"] or reason
                    if not c.ticker and ai["ticker"]:
                        c.ticker = normalize_ticker(ai["ticker"])
            if score < self.cfg.min_score:
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
        self.state.last_alert[key] = now
        self.state.dirty = True
        try:
            await self.tg.send(self.chat_id, format_alert(c, score, reason, now))
            self.stats["alerts"] += 1
            log.info("ALERT %s score=%s %s", key, score, c.title or c.items)
        except Exception as exc:  # noqa: BLE001
            if self.state.last_alert.get(key) == now:
                del self.state.last_alert[key]
            log.error("Sending alert for %s failed: %s", key, exc)

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
            lines.append("⏱ מצב --once (הרצה מתוזמנת)")
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
                lines.append(f"✅ {esc(name)} — {last}")
            else:
                lines.append(f"❌ {esc(name)} — {esc(s.get('error', ''))} ({last})")
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
        elif cmd == "/list":
            wl = self.state.watchlist
            await self.reply("📋 רשימת מעקב: " + (", ".join(wl) if wl else "ריקה"))
        elif cmd == "/status":
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
        while not self.stop_event.is_set():
            started = time.monotonic()
            await asyncio.gather(*(self.poll_wire(u) for u in self.cfg.wire_feeds))
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

    async def run(self) -> None:
        await self.refresh_tickers()
        self.state.dirty = True
        self.save_state()
        if self.chat_id:
            await self.reply("🟢 <b>Stock News Radar פעיל</b>\n" + self.help_text())
        else:
            log.warning("TELEGRAM_CHAT_ID is empty — open the bot in Telegram and press Start")
        loops = [self.edgar_loop(), self.wire_loop(), self.command_loop(),
                 self.ticker_refresh_loop(), self.save_loop()]
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
        for form in self.cfg.edgar_forms:
            await self.poll_edgar(form)
        await asyncio.gather(*(self.poll_wire(u) for u in self.cfg.wire_feeds))
        await self.drain()
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
        radar = Radar(cfg, client, once=args.once)
        if args.once:
            return await radar.run_once()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, radar.stop_event.set)
            except (NotImplementedError, RuntimeError):
                pass
        await radar.run()
        return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stock News Radar — Telegram bot")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--test", action="store_true", help="check token, detect chat id, send a sample alert")
    group.add_argument("--once", action="store_true", help="single pass over all sources, then exit")
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
    if args.once and not cfg.chat_id:
        missing.append("TELEGRAM_CHAT_ID (חובה במצב --once)")
    if missing:
        print("❌ חסרים משתני סביבה: " + ", ".join(missing))
        return 2
    try:
        return asyncio.run(_main_async(args, cfg))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
