"""Offline tests for bot.py — every HTTP call goes to an httpx.MockTransport.

Run: python -m unittest discover -s tests -v
"""
from __future__ import annotations

import asyncio
import json
import math
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any, Callable
from unittest import mock

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

TICKERS_EXCHANGE = {
    "fields": ["cik", "name", "ticker", "exchange"],
    "data": [
        [1849056, "Oklo Inc.", "OKLO", "NYSE"],
        [1111111, "Dilute Corp", "DLUT", "Nasdaq"],
        [2222222, "Pink Sheet Co", "PINK", "OTC"],
        [3333333, "Target Bio Inc.", "TBIO", "Nasdaq"],
        [4444444, "Teva Pharmaceutical Industries Ltd", "TEVA", "NYSE"],
    ],
}


def atom_entry(form: str, company: str, cik: int, acc: str, items: list[str],
               updated: str = "2026-09-25T06:00:00-04:00") -> str:
    items_html = "".join(f"&lt;br&gt;Item {i}: Something" for i in items)
    return f"""<entry>
<title>{form} - {company} ({cik:010d}) (Filer)</title>
<link rel="alternate" type="text/html" href="https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-', '')}/{acc}-index.htm"/>
<summary type="html"> &lt;b&gt;Filed:&lt;/b&gt; 2026-09-25 &lt;b&gt;AccNo:&lt;/b&gt; {acc} &lt;b&gt;Size:&lt;/b&gt; 300 KB{items_html}</summary>
<updated>{updated}</updated>
<category scheme="https://www.sec.gov/" label="form type" term="{form}"/>
<id>urn:tag:sec.gov,2008:accession-number={acc}</id>
</entry>"""


def atom_feed(entries: list[str]) -> str:
    return ('<?xml version="1.0" encoding="ISO-8859-1" ?>\n'
            '<feed xmlns="http://www.w3.org/2005/Atom"><title>Latest Filings</title>'
            + "".join(entries) + "</feed>")


def index_page(docs: list[tuple[str, str]]) -> str:
    rows = "".join(
        f'<tr><td scope="row">{i + 1}</td><td scope="row">desc</td>'
        f'<td scope="row"><a href="{href}">{href.rsplit("/", 1)[-1]}</a></td>'
        f'<td scope="row">{typ}</td><td scope="row">1000</td></tr>'
        for i, (href, typ) in enumerate(docs)
    )
    return (f'<html><body><table class="tableFile" summary="Document Format Files">'
            f"<tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th></tr>"
            f"{rows}</table></body></html>")


def rss_item(guid: str, title: str, desc: str, link: str, pub: str = "Thu, 25 Sep 2026 10:00:00 GMT") -> str:
    return (f"<item><title>{title}</title><link>{link}</link><description>{desc}</description>"
            f"<guid>{guid}</guid><pubDate>{pub}</pubDate></item>")


def rss_feed(items: list[str]) -> str:
    return ('<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel>'
            "<title>News</title>" + "".join(items) + "</channel></rss>")


CONTRACT_PR = """<html><body><div>menu class action lawsuit sidebar</div>
<div class="release-body container"><p>WASHINGTON, Sept. 25, 2026 /PRNewswire/ -- Oklo Inc. (NYSE: OKLO)
today announced it has been awarded a $450 million contract by the U.S. Department of Defense to deploy
advanced fission power at military installations.</p>
<p>The award is the largest in the company's history.</p>
<p>About Oklo</p><p>Oklo is a company. Risks include going concern and dilution.</p>
<p>Forward-Looking Statements</p><p>This release may contain a public offering statement.</p></div>
</body></html>"""

CONTRACT_EX99 = """<html><body><p>Exhibit 99.1</p><p>Oklo Awarded $450 Million Contract by U.S. Department
of Defense</p><p>SANTA CLARA, Calif., Sept. 25, 2026 -- Oklo Inc. (NYSE: OKLO) today announced it has been awarded
a $450 million contract by the U.S. Department of Defense.</p><p>About Oklo</p><p>Oklo may face a class action.</p>
</body></html>"""

OFFERING_PR = """<html><body><div class="release-body"><p>NEW YORK /PRNewswire/ -- Dilute Corp
(NASDAQ: DLUT) today announced the pricing of its $20 million underwritten public offering of common stock.
The company was awarded a grant by NASA.</p></div></body></html>"""

CONFERENCE_PR = """<html><body><div class="release-body"><p>Target Bio Inc. (NASDAQ: TBIO) will present at
the Jefferies Healthcare Conference on October 3.</p></div></body></html>"""


# ---------------------------------------------------------------------------
# Mock HTTP world
# ---------------------------------------------------------------------------


class World:
    """URL -> response routing plus a record of Telegram messages."""

    def __init__(self) -> None:
        self.routes: dict[str, Callable[[httpx.Request], httpx.Response] | tuple[int, Any]] = {}
        self.sent: list[dict[str, Any]] = []
        self.requests: list[httpx.Request] = []
        self.updates: list[dict[str, Any]] = []
        self.get_updates_payloads: list[dict[str, Any]] = []
        self.send_status = 200
        self.bad_chats: set[str] = set()
        self.me_status = 200
        self.claude: Callable[[dict[str, Any]], httpx.Response] | None = None
        self.claude_calls: list[dict[str, Any]] = []

    def set(self, url: str, body: Any, status: int = 200) -> None:
        self.routes[url] = (status, body)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        url = str(request.url)
        if url.startswith("https://api.telegram.org/"):
            return self.telegram(request)
        if url == bot.ANTHROPIC_URL:
            payload = json.loads(request.content)
            self.claude_calls.append(payload)
            assert self.claude is not None
            return self.claude(payload)
        route = self.routes.get(url)
        if route is None:  # keys ending in "*" match by prefix
            route = next((v for k, v in self.routes.items() if k.endswith("*") and url.startswith(k[:-1])), None)
        if route is None:
            return httpx.Response(404, text="not found")
        if callable(route):
            return route(request)
        status, body = route
        if isinstance(body, (dict, list)):
            return httpx.Response(status, json=body)
        return httpx.Response(status, content=body.encode("utf-8") if isinstance(body, str) else body)

    def telegram(self, request: httpx.Request) -> httpx.Response:
        method = str(request.url).rsplit("/", 1)[-1]
        payload = json.loads(request.content or b"{}")
        if method == "getMe":
            if self.me_status != 200:
                return httpx.Response(self.me_status, json={"ok": False, "error_code": self.me_status,
                                                            "description": "Unauthorized"})
            return httpx.Response(200, json={"ok": True, "result": {"id": 1, "username": "radar_bot"}})
        if method == "getUpdates":
            self.get_updates_payloads.append(payload)
            offset = payload.get("offset", 0)
            result = [u for u in self.updates if u["update_id"] >= offset]
            return httpx.Response(200, json={"ok": True, "result": result})
        if method == "sendMessage":
            if str(payload.get("chat_id")) in self.bad_chats:
                return httpx.Response(400, json={
                    "ok": False, "error_code": 400, "description": "Bad Request: chat not found"})
            if self.send_status != 200:
                return httpx.Response(self.send_status, json={
                    "ok": False, "error_code": self.send_status, "description": "Bad Request: chat not found"})
            self.sent.append(payload)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": len(self.sent)}})
        return httpx.Response(404, json={"ok": False, "description": "no method"})

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handler), follow_redirects=True)


def make_cfg(tmp: Path, **overrides: Any) -> bot.Config:
    overrides.setdefault("guru_alerts", "gurus" in overrides)   # off unless a test sets gurus
    overrides.setdefault("jump_model", False)                  # tests set a model explicitly
    cfg = bot.Config(
        telegram_token="123:ABC",
        chat_id="42",
        sec_user_agent="Test Tester test@example.com",
        wire_feeds=["https://www.prnewswire.com/rss/news-releases-list.rss"],
        edgar_forms=["8-K", "6-K"],
        state_file=tmp / "state.json",
        env_file=tmp / ".env",
    )
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


EDGAR_8K = bot.EDGAR_FEED_URL.format(form="8-K")
EDGAR_6K = bot.EDGAR_FEED_URL.format(form="6-K")
PRN = "https://www.prnewswire.com/rss/news-releases-list.rss"


def base_world() -> World:
    w = World()
    w.set(bot.TICKERS_EXCHANGE_URL, TICKERS_EXCHANGE)
    w.set(EDGAR_8K, atom_feed([atom_entry("8-K", "Old Co", 1849056, "0001849056-26-000001", ["8.01"])]))
    w.set(EDGAR_6K, atom_feed([atom_entry("6-K", "TEVA", 4444444, "0004444444-26-000001", [])]))
    w.set(PRN, rss_feed([rss_item("old-1", "Old news", "Oklo Inc. (NYSE: OKLO) old", "https://www.prnewswire.com/old-1.html")]))
    return w


def run(coro: Any) -> Any:
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


class RulesTest(unittest.TestCase):
    def test_dod_contract_scores_5(self) -> None:
        text = "Oklo Awarded $450 Million Contract by U.S. Department of Defense"
        self.assertEqual(bot.rule_score(text, "Oklo Inc.").score, 5)
        self.assertIsNone(bot.negative_hit(text))

    def test_cash_acquisition_scores_5(self) -> None:
        text = "Target Bio to be Acquired by BigPharma for $42.00 per share in cash"
        res = bot.rule_score(text)
        self.assertEqual(res.score, 5)
        self.assertIn("החברה נרכשת", res.reason_he)

    def test_fda_approval_scores_5(self) -> None:
        self.assertEqual(bot.rule_score("FDA Approves Target Bio's Drug for Rare Disease").score, 5)

    def test_conference_scores_0(self) -> None:
        text = "Target Bio to Present at the Jefferies Healthcare Conference"
        self.assertEqual(bot.rule_score(text).score, 0)

    def test_offering_rejected(self) -> None:
        self.assertEqual(bot.negative_hit("Dilute Corp Announces Pricing of $20 Million Public Offering"),
                         "הנפקה ודילול")
        self.assertIsNotNone(bot.negative_hit("Dilute Corp announces $5 million registered direct offering"))

    def test_class_action_rejected(self) -> None:
        self.assertIsNotNone(bot.negative_hit(
            "INVESTOR ALERT: Rosen Law Firm Encourages OKLO Investors to Secure Counsel - class action"))

    def test_other_negative_categories(self) -> None:
        for text in ("announces 1-for-20 reverse stock split",
                     "received a deficiency letter regarding the minimum bid price",
                     "substantial doubt about its ability to continue as a going concern",
                     "files voluntary Chapter 11 petitions",
                     "will restate its previously issued financial statements",
                     "lowers full-year guidance",
                     "FDA places clinical hold on trial",
                     "receives Complete Response Letter from FDA",
                     "CEO steps down; company announces workforce reduction"):
            self.assertIsNotNone(bot.negative_hit(text), text)

    def test_merger_boilerplate_not_rejected(self) -> None:
        text = ("Target Bio to be acquired by BigPharma. Upon completion, Target Bio's shares will no "
                "longer be listed on Nasdaq. Amended and Restated Bylaws will be adopted.")
        self.assertIsNone(bot.negative_hit(text))

    def test_strong_phrases_need_headline_or_opening(self) -> None:
        title = "Nyxoah Appoints Liam Kelly as Chief Executive Officer"
        body = ("MONT-SAINT-GUIBERT, Belgium, Sept. 28, 2026 (GLOBE NEWSWIRE) -- Nyxoah SA (Nasdaq: NYXH) "
                "today announced the appointment of Liam Kelly as CEO. The company commercialises Genio "
                "in the U.S. following FDA approval in 2025.")
        res = bot.rule_score(f"{title}\n{body}", "Nyxoah SA", strong_text=f"{title}\n{body[:400]}",
                             title=title)
        self.assertEqual(res.score, 0)
        late = "x" * 500 + " The company recently received FDA approval for another product."
        res = bot.rule_score(f"Acme reports update\n{late}", strong_text=f"Acme reports update\n{late[:400]}")
        self.assertEqual(res.score, 0)
        opening = "Acme today announced FDA approval of its drug."
        res = bot.rule_score(f"Acme update\n{opening}", strong_text=f"Acme update\n{opening}")
        self.assertEqual(res.score, 5)

    def test_bonuses_only_with_positive_score(self) -> None:
        self.assertEqual(bot.rule_score("Company mentions NVIDIA and $900 million").score, 0)
        res = bot.rule_score("Company announces strategic partnership with NVIDIA")
        self.assertEqual(res.score, 4)  # 2 + mega-company bonus 2
        self.assertEqual(bot.rule_score("NVIDIA announces strategic partnership", "NVIDIA Corp").score, 2)

    def test_amount_bonus(self) -> None:
        # receiving orders scores 3 since the gainers study (it moves small caps); +1 at $100M+
        self.assertEqual(bot.rule_score("Receives purchase order worth $120M").score, 4)
        self.assertEqual(bot.rule_score("Receives purchase order worth $12M").score, 3)
        self.assertEqual(bot.rule_score("Receives purchase order worth $1.5 billion").score, 4)
        self.assertEqual(bot.rule_score("Purchase orders remain strong").score, 2)

    def test_rules_learned_from_gainers(self) -> None:
        def score(title: str, company: str = "") -> int:
            return bot.rule_score(title, company, title=title).score
        self.assertEqual(score("Volato Subsidiary Signs $1.2 Billion in AI Infrastructure Orders"), 4)
        self.assertEqual(score("Kandi Secures Follow-On Order for CATL Batteries"), 3)
        self.assertEqual(score("Nexalin Signs Definitive Exclusive Distribution and Local Manufacturing Agreement"), 3)
        self.assertEqual(score("HeartBeam Receives FDA Breakthrough Device Designation"), 4)
        self.assertEqual(score("Acme Receives FDA 510(k) Clearance for Its Pump"), 4)
        self.assertEqual(score("C.H. Robinson to Acquire RXO, Redefining Logistics", "RXO, Inc."), 5)
        self.assertEqual(score("C.H. Robinson to Acquire RXO, Redefining Logistics", "C.H. Robinson Worldwide"), 0)
        self.assertEqual(score("TGE's profit surged by 9.9 times"), 3)
        self.assertEqual(score("BIO-key Partners with Al Majlis Group"), 2)
        self.assertEqual(score("Acme Announces Strategic Partnership with Beta"), 2)   # counted once
        self.assertEqual(score("Aethlon Medical & North Immunology Announce Merger to Advance IL-13"), 3)
        self.assertEqual(score("Sono Group and Sports One Sign Letter of Intent to Combine"), 3)

    def test_claude_json_parsing(self) -> None:
        self.assertEqual(bot.parse_claude_json('```json\n{"score": 7, "ticker": "X", "reason_he": "א"}\n```'),
                         {"score": 5, "ticker": "X", "reason_he": "א"})
        self.assertIsNone(bot.parse_claude_json("no json here"))
        self.assertIsNone(bot.parse_claude_json('{"ticker": "X"}'))


class ParsingTest(unittest.TestCase):
    def test_extract_tickers(self) -> None:
        text = ("Oklo Inc. (NYSE: OKLO) and Energy Fuels (NYSE American: UUUU) with NVIDIA (Nasdaq GS: NVDA), "
                "Canada Co (TSX: CAN) (OTCQB: CANN), Berkshire (NYSE:BRK.B), GLOBE NASDAQ:ABCD")
        self.assertEqual(bot.extract_tickers(text), ["OKLO", "UUUU", "NVDA", "BRK-B", "ABCD"])

    def test_pick_document_prefers_ex99_1(self) -> None:
        page = index_page([
            ("/Archives/edgar/data/1/0001/form8k.htm", "8-K"),
            ("/Archives/edgar/data/1/0001/ex99-2.htm", "EX-99.2"),
            ("/Archives/edgar/data/1/0001/ex99-1.htm", "EX-99.1"),
            ("/Archives/edgar/data/1/0001/ex10-1.htm", "EX-10.1"),
        ])
        url, is_ex = bot.pick_document(page, "https://www.sec.gov/Archives/edgar/data/1/0001/x-index.htm")
        self.assertEqual(url, "https://www.sec.gov/Archives/edgar/data/1/0001/ex99-1.htm")
        self.assertTrue(is_ex)

    def test_pick_document_falls_back_to_8k(self) -> None:
        page = index_page([
            ("/ix?doc=/Archives/edgar/data/1/0001/form8k.htm", "8-K"),
            ("/Archives/edgar/data/1/0001/ex10-1.htm", "EX-10.1"),
        ])
        url, is_ex = bot.pick_document(page, "https://www.sec.gov/x-index.htm")
        self.assertEqual(url, "https://www.sec.gov/Archives/edgar/data/1/0001/form8k.htm")
        self.assertFalse(is_ex)

    def test_parse_edgar_feed(self) -> None:
        feed = atom_feed([
            atom_entry("8-K", "Oklo Inc.", 1849056, "0001849056-26-000002", ["1.01", "9.01"]),
            atom_entry("8-K/A", "Oklo Inc.", 1849056, "0001849056-26-000003", ["8.01"]),
        ])
        filings = bot.parse_edgar_feed(feed.encode("latin-1"))
        self.assertEqual(len(filings), 2)
        f = filings[0]
        self.assertEqual((f.form, f.ciks, f.accession, f.items), ("8-K", [1849056], "0001849056-26-000002",
                                                                 ["1.01", "9.01"]))
        self.assertAlmostEqual(f.filed_ts, 1790330400.0)  # 2026-09-25T10:00:00Z
        self.assertEqual(filings[1].form, "8-K/A")

    def test_strip_boilerplate_and_article(self) -> None:
        text = bot.strip_boilerplate(bot.extract_article_text(CONTRACT_PR))
        self.assertIn("awarded a $450 million contract", text)
        self.assertNotIn("going concern", text)
        self.assertNotIn("sidebar", text)

    def test_ticker_map_excludes_otc(self) -> None:
        tm = bot.TickerMap()
        tm.load(TICKERS_EXCHANGE)
        self.assertEqual(tm.tickers_for_cik(1849056), ["OKLO"])
        self.assertIsNone(tm.lookup("PINK"))
        tm.load({"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}})
        self.assertEqual(tm.lookup("aapl"), (320193, "Apple Inc."))


class FormatTest(unittest.TestCase):
    def test_wire_alert_format(self) -> None:
        now = time.time()
        c = bot.Candidate(source="wire", source_label="PR Newswire", ticker="OKLO", company="Oklo Inc.",
                          title="Oklo Awarded $450 Million Contract <DoD>", link="https://x.test/a?b=1&c=2",
                          published_ts=now - 8, watch=False)
        msg = bot.format_alert(c, 5, "חוזה ענק", now)
        lines = msg.split("\n")
        self.assertEqual(lines[0], "🚀🚀 חיובי מאוד (+5)")
        self.assertEqual(lines[1], "<b>OKLO</b> | Oklo Inc.")
        self.assertEqual(lines[2], "Oklo Awarded $450 Million Contract &lt;DoD&gt;")
        self.assertEqual(lines[3], "💡 חוזה ענק")
        self.assertEqual(lines[4], "📰 PR Newswire · ⏱ 8 שניות מהפרסום")
        self.assertEqual(lines[5], '<a href="https://x.test/a?b=1&amp;c=2">למקור המלא</a>')

    def test_sec_alert_items_and_no_timer_for_old(self) -> None:
        now = time.time()
        c = bot.Candidate(source="sec", source_label="SEC EDGAR · 8-K", ticker="OKLO", company="Oklo Inc.",
                          title="", link="https://sec.test/x", published_ts=now - 7200, watch=False,
                          form="8-K", items=["1.01", "9.01"])
        msg = bot.format_alert(c, 4, "", now)
        self.assertIn("Item 1.01: חתימה על הסכם מהותי", msg)
        self.assertIn("Item 9.01: דוחות כספיים ונספחים", msg)
        self.assertNotIn("⏱", msg)
        self.assertNotIn("💡", msg)


# ---------------------------------------------------------------------------
# Pipeline tests
# ---------------------------------------------------------------------------


class PipelineTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.world = base_world()
        patcher = mock.patch.object(bot, "SEC_MIN_INTERVAL", 0.0)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    async def cycle(self, radar: bot.Radar) -> None:
        # Drain after each source so the EDGAR alert deterministically wins the race.
        for form in radar.cfg.edgar_forms:
            await radar.poll_edgar(form)
            await radar.drain()
        for url in radar.cfg.wire_feeds:
            await radar.poll_wire(url)
            await radar.drain()

    def add_new_items(self) -> None:
        w = self.world
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(time.time() - 5))
        acc_contract = "0001849056-26-000010"
        acc_offering = "0001111111-26-000011"
        acc_otc = "0002222222-26-000012"
        w.set(EDGAR_8K, atom_feed([
            atom_entry("8-K", "Oklo Inc.", 1849056, acc_contract, ["1.01", "9.01"], now_iso),
            atom_entry("8-K", "Dilute Corp", 1111111, acc_offering, ["8.01"], now_iso),
            atom_entry("8-K", "Pink Sheet Co", 2222222, acc_otc, ["1.01"], now_iso),
            atom_entry("8-K", "Old Co", 1849056, "0001849056-26-000001", ["8.01"]),
        ]))
        base = "https://www.sec.gov/Archives/edgar/data"
        w.set(f"{base}/1849056/{acc_contract.replace('-', '')}/{acc_contract}-index.htm",
              index_page([("/Archives/edgar/data/1849056/x/oklo8k.htm", "8-K"),
                          ("/Archives/edgar/data/1849056/x/ex99.htm", "EX-99.1")]))
        w.set(f"{base}/1849056/x/ex99.htm", CONTRACT_EX99)
        w.set(f"{base}/1111111/{acc_offering.replace('-', '')}/{acc_offering}-index.htm",
              index_page([("/Archives/edgar/data/1111111/x/ex99.htm", "EX-99.1")]))
        w.set(f"{base}/1111111/x/ex99.htm", OFFERING_PR)
        # Same OKLO news on the wire (should be de-duplicated), plus a neutral PR.
        w.set(PRN, rss_feed([
            rss_item("prn-2", "Oklo Awarded $450 Million Contract by U.S. Department of Defense",
                     "Oklo Inc. (NYSE: OKLO) today announced", "https://www.prnewswire.com/oklo.html"),
            rss_item("prn-3", "Target Bio to Present at Jefferies Conference",
                     "Target Bio Inc. (NASDAQ: TBIO) will present", "https://www.prnewswire.com/tbio.html"),
            rss_item("prn-4", "Private Co raises seed round", "No ticker here",
                     "https://www.prnewswire.com/private.html"),
            rss_item("old-1", "Old news", "Oklo Inc. (NYSE: OKLO) old", "https://www.prnewswire.com/old-1.html"),
        ]))
        w.set("https://www.prnewswire.com/oklo.html", CONTRACT_PR)
        w.set("https://www.prnewswire.com/tbio.html", CONFERENCE_PR)

    def test_full_pipeline_rules(self) -> None:
        async def scenario() -> None:
            async with self.world.client() as client:
                radar = bot.Radar(make_cfg(self.tmp), client)
                self.assertTrue(await radar.refresh_tickers())
                await self.cycle(radar)  # first run: initialise only
                self.assertEqual(self.world.sent, [])
                self.assertEqual(radar.state.initialized,
                                 {"edgar:8-K", "edgar:6-K", f"wire:{PRN}"})
                self.add_new_items()
                await self.cycle(radar)
                self.assertEqual(len(self.world.sent), 1, [m["text"] for m in self.world.sent])
                msg = self.world.sent[0]
                self.assertEqual(msg["chat_id"], "42")
                self.assertEqual(msg["parse_mode"], "HTML")
                self.assertEqual(msg["link_preview_options"], {"is_disabled": True})
                self.assertIn("🚀🚀 חיובי מאוד (+5)", msg["text"])
                self.assertIn("<b>OKLO</b> | Oklo Inc.", msg["text"])
                self.assertIn("Item 1.01: חתימה על הסכם מהותי", msg["text"])
                self.assertIn("⏱", msg["text"])
                radar.state.save()

            # Restart: nothing is sent again.
            self.world.sent.clear()
            async with self.world.client() as client:
                radar2 = bot.Radar(make_cfg(self.tmp), client)
                await radar2.refresh_tickers()
                await self.cycle(radar2)
                self.assertEqual(self.world.sent, [])
                self.assertIn("OKLO", radar2.state.last_alert)

        run(scenario())
        # SEC requests carry the configured User-Agent.
        sec_reqs = [r for r in self.world.requests if r.url.host == "www.sec.gov"]
        self.assertTrue(sec_reqs)
        self.assertTrue(all(r.headers["User-Agent"] == "Test Tester test@example.com" for r in sec_reqs))
        # Wire requests identify honestly as an RSS reader, not as a browser.
        wire_reqs = [r for r in self.world.requests if r.url.host == "www.prnewswire.com"]
        self.assertTrue(wire_reqs)
        self.assertTrue(all(r.headers["User-Agent"] == bot.WIRE_USER_AGENT for r in wire_reqs))
        self.assertNotIn("Mozilla", bot.WIRE_USER_AGENT)

    def test_claude_engine_and_fallback(self) -> None:
        def claude(payload: dict[str, Any]) -> httpx.Response:
            content = payload["messages"][0]["content"]
            if "Ticker: OKLO" in content:
                return httpx.Response(200, json={"content": [{"type": "text", "text": json.dumps(
                    {"score": 5, "ticker": "OKLO", "reason_he": "חוזה ענק מול משרד ההגנה"})}]})
            return httpx.Response(500, json={"error": "boom"})

        self.world.claude = claude

        async def scenario() -> None:
            async with self.world.client() as client:
                cfg = make_cfg(self.tmp, anthropic_key="sk-test", dedup_hours=0)
                radar = bot.Radar(cfg, client)
                await radar.refresh_tickers()
                await self.cycle(radar)
                self.add_new_items()
                await self.cycle(radar)
                return radar

        radar = run(scenario())
        # Only items with rules score >= 1 reach Claude: OKLO via EDGAR and via the wire.
        tickers = sorted(p["messages"][0]["content"].split("Ticker: ")[1].split("\n")[0]
                         for p in self.world.claude_calls)
        self.assertEqual(tickers, ["OKLO", "OKLO"])
        self.assertEqual(self.world.claude_calls[0]["model"], bot.DEFAULT_MODEL)
        self.assertLessEqual(len(self.world.claude_calls[0]["messages"][0]["content"]), 6200)
        self.assertEqual(len(self.world.sent), 2)  # dedup disabled in this test
        self.assertIn("💡 חוזה ענק מול משרד ההגנה", self.world.sent[0]["text"])
        self.assertEqual(radar.stats["ai_calls"], 2)

    def test_claude_failure_falls_back_to_rules(self) -> None:
        self.world.claude = lambda payload: httpx.Response(529, json={"error": "overloaded"})

        async def scenario() -> bot.Radar:
            async with self.world.client() as client:
                radar = bot.Radar(make_cfg(self.tmp, anthropic_key="sk-test"), client)
                await radar.refresh_tickers()
                await self.cycle(radar)
                self.add_new_items()
                await self.cycle(radar)
                return radar

        radar = run(scenario())
        self.assertEqual(len(self.world.sent), 1)
        self.assertIn("(+5)", self.world.sent[0]["text"])
        self.assertGreaterEqual(radar.stats["ai_errors"], 1)

    def test_watchlist_only_and_positive_only_false(self) -> None:
        async def scenario() -> None:
            async with self.world.client() as client:
                cfg = make_cfg(self.tmp, marketwide=False, positive_only=False, watchlist=["TBIO"])
                radar = bot.Radar(cfg, client)
                await radar.refresh_tickers()
                await self.cycle(radar)
                self.add_new_items()
                await self.cycle(radar)

        run(scenario())
        self.assertEqual(len(self.world.sent), 1)
        self.assertIn("📄 דיווח חדש (רשימת מעקב)", self.world.sent[0]["text"])
        self.assertIn("TBIO", self.world.sent[0]["text"])

    def test_sec_block_pauses_requests(self) -> None:
        self.world.set(EDGAR_8K, "blocked", status=403)

        async def scenario() -> bot.Radar:
            async with self.world.client() as client:
                radar = bot.Radar(make_cfg(self.tmp), client)
                await radar.poll_edgar("8-K")
                n = len(self.world.requests)
                await radar.poll_edgar("8-K")
                self.assertEqual(len(self.world.requests), n)  # no request while blocked
                await radar.poll_wire(PRN)  # other sources keep working
                self.assertIn("SEC", radar.status_text())
                return radar

        radar = run(scenario())
        self.assertFalse(radar.state.sources["SEC 8-K"]["ok"])
        self.assertTrue(radar.state.sources["PR Newswire"]["ok"])
        self.assertGreater(radar.fetcher.sec_blocked_until, time.time() + 500)

    def test_etag_conditional_requests(self) -> None:
        calls: list[httpx.Request] = []

        def feed(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            if request.headers.get("If-None-Match") == '"v1"':
                return httpx.Response(304)
            return httpx.Response(200, headers={"ETag": '"v1"'}, content=rss_feed([]).encode())

        self.world.routes[PRN] = feed

        async def scenario() -> None:
            async with self.world.client() as client:
                f = bot.Fetcher(client, "UA x@y.z")
                self.assertIsNotNone(await f.get(PRN, conditional=True))
                self.assertIsNone(await f.get(PRN, conditional=True))

        run(scenario())
        self.assertEqual(calls[1].headers.get("If-None-Match"), '"v1"')


class CommandsTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.world = base_world()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    @staticmethod
    def update(uid: int, text: str, chat_id: int = 42, chat_type: str = "private") -> dict[str, Any]:
        return {"update_id": uid, "message": {"message_id": uid, "text": text,
                                              "chat": {"id": chat_id, "type": chat_type}}}

    def test_commands(self) -> None:
        self.world.updates = [
            self.update(1, "/add nvda $OKLO bad!"),
            self.update(2, "/remove NVDA"),
            self.update(3, "/list"),
            self.update(4, "/status"),
            self.update(5, "/add HACK", chat_id=999),  # foreign chat is ignored
            self.update(6, "/test"),
            self.update(7, "/help"),
        ]

        async def scenario() -> bot.Radar:
            async with self.world.client() as client:
                radar = bot.Radar(make_cfg(self.tmp), client)
                await radar.refresh_tickers()
                await radar.process_updates(timeout=0)
                return radar

        radar = run(scenario())
        self.assertEqual(radar.state.watchlist, ["OKLO"])
        self.assertEqual(radar.state.tg_offset, 8)
        texts = [m["text"] for m in self.world.sent]
        self.assertEqual(len(texts), 6)
        self.assertIn("✅ נוסף: NVDA, OKLO", texts[0])
        self.assertIn("❌ לא תקין: bad!", texts[0])
        self.assertIn("🗑 הוסר: NVDA", texts[1])
        self.assertEqual(texts[2], "📋 רשימת מעקב: OKLO")
        self.assertIn("זמן פעילות", texts[3])
        self.assertIn("✅ SEC Tickers", texts[3])
        self.assertIn("התראת דוגמה", texts[4])
        self.assertIn("/add NVDA OKLO", texts[5])

    def test_auto_detect_chat_id(self) -> None:
        self.world.updates = [self.update(1, "hello", chat_id=-100, chat_type="group"),
                              self.update(2, "/start", chat_id=777)]

        async def scenario() -> bot.Radar:
            async with self.world.client() as client:
                radar = bot.Radar(make_cfg(self.tmp, chat_id=""), client)
                await radar.process_updates(timeout=0)
                return radar

        with mock.patch.dict("os.environ", {}, clear=False):
            radar = run(scenario())
        self.assertEqual(radar.chat_id, "777")
        self.assertIn("TELEGRAM_CHAT_ID=777", (self.tmp / ".env").read_text())
        self.assertEqual(self.world.sent[0]["chat_id"], "777")
        self.assertIn("ננעל", self.world.sent[0]["text"])

    def test_save_env_var_replaces_existing(self) -> None:
        env = self.tmp / ".env"
        env.write_text("TELEGRAM_BOT_TOKEN=abc\nTELEGRAM_CHAT_ID=\nMIN_SCORE=4\n")
        with mock.patch.dict("os.environ", {}, clear=False):
            bot.save_env_var(env, "TELEGRAM_CHAT_ID", "55")
        self.assertEqual(env.read_text(), "TELEGRAM_BOT_TOKEN=abc\nTELEGRAM_CHAT_ID=55\nMIN_SCORE=4\n")

    def test_once_mode(self) -> None:
        self.world.updates = [self.update(10, "/add TBIO")]

        async def first_run() -> None:
            async with self.world.client() as client:
                await bot.Radar(make_cfg(self.tmp), client, once=True).run_once()

        run(first_run())
        state = json.loads((self.tmp / "state.json").read_text())
        self.assertEqual(state["watchlist"], ["TBIO"])
        self.assertEqual(state["tg_offset"], 11)
        self.assertEqual(len(state["initialized"]), 3)
        self.assertEqual(len(self.world.sent), 1)  # only the /add reply, no startup message

        # Second run: new items; the pending /add must not be processed again.
        self.world.sent.clear()
        PipelineTest.add_new_items(self)  # type: ignore[arg-type]

        async def second_run() -> None:
            async with self.world.client() as client:
                await bot.Radar(make_cfg(self.tmp), client, once=True).run_once()

        with mock.patch.object(bot, "SEC_MIN_INTERVAL", 0.0):
            run(second_run())
        self.assertEqual(self.world.get_updates_payloads[-1]["offset"], 11)
        self.assertEqual(len(self.world.sent), 1)
        self.assertIn("OKLO", self.world.sent[0]["text"])

    def test_once_answers_status_after_polling(self) -> None:
        self.world.updates = [self.update(20, "/status")]

        async def scenario() -> None:
            async with self.world.client() as client:
                await bot.Radar(make_cfg(self.tmp), client, once=True).run_once()

        with mock.patch.object(bot, "SEC_MIN_INTERVAL", 0.0):
            run(scenario())
        self.assertEqual(len(self.world.sent), 1)
        text = self.world.sent[0]["text"]
        self.assertIn("הנתונים של ההרצה הזו", text)
        self.assertIn("✅ SEC 8-K — עודכן לפני", text)  # fresh from this run, not the previous one
        self.assertNotIn("עוד לא נדגם", text)

    def test_once_requires_chat_id(self) -> None:
        env = {"TELEGRAM_BOT_TOKEN": "1:A", "SEC_USER_AGENT": "a b@c.d", "TELEGRAM_CHAT_ID": "",
               "ENV_FILE": str(self.tmp / ".env")}
        with mock.patch.dict("os.environ", env, clear=False):
            self.assertEqual(bot.main(["--once"]), 2)


class TestModeTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.world = World()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def run_test_mode(self, cfg: bot.Config) -> int:
        async def scenario() -> int:
            async with self.world.client() as client:
                return await bot.run_test(cfg, client)
        with mock.patch.dict("os.environ", {}, clear=False):
            return run(scenario())

    def test_success_with_detection(self) -> None:
        self.world.updates = [{"update_id": 1, "message": {"text": "/start", "chat": {"id": 555, "type": "private"}}}]
        self.assertEqual(self.run_test_mode(make_cfg(self.tmp, chat_id="")), 0)
        self.assertIn("TELEGRAM_CHAT_ID=555", (self.tmp / ".env").read_text())
        self.assertIn("התראת דוגמה", self.world.sent[0]["text"])

    def test_no_chat_found(self) -> None:
        self.assertEqual(self.run_test_mode(make_cfg(self.tmp, chat_id="")), 1)

    def test_bad_token_401(self) -> None:
        self.world.me_status = 401
        self.assertEqual(self.run_test_mode(make_cfg(self.tmp)), 1)

    def test_bad_chat_400(self) -> None:
        self.world.send_status = 400
        self.assertEqual(self.run_test_mode(make_cfg(self.tmp)), 1)

    def test_wrong_chat_id_detects_and_sends_privately(self) -> None:
        self.world.bad_chats = {"42"}
        self.world.updates = [{"update_id": 1, "message": {"text": "/start", "chat": {"id": 555, "type": "private"}}}]
        with mock.patch("builtins.print") as printed:
            self.assertEqual(self.run_test_mode(make_cfg(self.tmp)), 1)
        self.assertEqual(self.world.sent[0]["chat_id"], "555")
        self.assertIn("<code>555</code>", self.world.sent[0]["text"])
        self.assertNotIn("555", " ".join(str(c) for c in printed.call_args_list))  # not in public logs
        self.assertFalse((self.tmp / ".env").exists())

    def test_chat_id_equal_to_bot_id(self) -> None:
        self.world.updates = [{"update_id": 1, "message": {"text": "hi", "chat": {"id": 555, "type": "private"}}}]
        self.assertEqual(self.run_test_mode(make_cfg(self.tmp, chat_id="1")), 1)  # getMe id is 1
        self.assertEqual(self.world.sent[0]["chat_id"], "555")

    def test_wrong_chat_id_and_no_updates(self) -> None:
        self.world.bad_chats = {"42"}
        self.assertEqual(self.run_test_mode(make_cfg(self.tmp)), 1)
        self.assertEqual(self.world.sent, [])

    def test_blocked_403(self) -> None:
        self.world.send_status = 403
        self.assertEqual(self.run_test_mode(make_cfg(self.tmp)), 1)


class RobustnessTest(unittest.TestCase):
    def test_clean_token(self) -> None:
        good = "123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsaw"
        self.assertEqual(bot.clean_token(f'  "{good}"\n'), good)
        self.assertEqual(bot.clean_token(f"bot{good}"), good)
        self.assertEqual(bot.clean_token(good[:20] + " " + good[20:]), good)

    def test_token_hint_does_not_leak(self) -> None:
        bad = "not-a-token-secret"
        hint = bot.token_hint(bad)
        self.assertNotIn(bad, hint)
        self.assertIn("לא בפורמט", hint)
        self.assertIn("בוטל", bot.token_hint("123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsaw"))
        self.assertIn("רק החלק שאחרי הנקודתיים", bot.token_hint("AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsawX"))

    def test_wire_retry_then_success(self) -> None:
        world = base_world()
        calls: list[int] = []

        def flaky(request: httpx.Request) -> httpx.Response:
            calls.append(1)
            if len(calls) == 1:
                return httpx.Response(502, text="bad gateway")
            return httpx.Response(200, content=rss_feed([rss_item("a", "t", "d", "https://x.test/a")]).encode())

        world.routes[PRN] = flaky
        with tempfile.TemporaryDirectory() as d, mock.patch.object(asyncio, "sleep", new=mock.AsyncMock()):
            async def scenario() -> bot.Radar:
                async with world.client() as client:
                    radar = bot.Radar(make_cfg(Path(d)), client)
                    await radar.poll_wire(PRN)
                    return radar
            radar = run(scenario())
        self.assertEqual(len(calls), 2)
        self.assertTrue(radar.state.sources["PR Newswire"]["ok"])

    def test_wire_redirect_is_retried_not_followed(self) -> None:
        world = base_world()
        calls: list[str] = []

        def redirect_then_ok(request: httpx.Request) -> httpx.Response:
            calls.append(str(request.url))
            if len(calls) == 1:
                return httpx.Response(301, headers={"Location": "https://www.prnewswire.com/broken"})
            return httpx.Response(200, content=rss_feed([rss_item("a", "t", "d", "https://x.test/a")]).encode())

        world.routes[PRN] = redirect_then_ok
        with tempfile.TemporaryDirectory() as d, mock.patch.object(asyncio, "sleep", new=mock.AsyncMock()):
            async def scenario() -> bot.Radar:
                async with world.client() as client:
                    radar = bot.Radar(make_cfg(Path(d)), client)
                    await radar.poll_wire(PRN)
                    return radar
            radar = run(scenario())
        self.assertEqual(calls, [PRN, PRN])  # the broken redirect target was never requested
        self.assertTrue(radar.state.sources["PR Newswire"]["ok"])

    def test_redirect_error_shows_location(self) -> None:
        req = httpx.Request("GET", PRN)
        resp = httpx.Response(301, headers={"Location": "https://x.test/moved"}, request=req)
        err = httpx.HTTPStatusError("x", request=req, response=resp)
        self.assertEqual(bot.describe_error(err), "HTTP 301 → https://x.test/moved")

    def test_feed_overflow_is_detected(self) -> None:
        world = base_world()
        with tempfile.TemporaryDirectory() as d:
            async def scenario() -> bot.Radar:
                async with world.client() as client:
                    radar = bot.Radar(make_cfg(Path(d)), client)
                    await radar.poll_wire(PRN)            # first run: initialise
                    world.set(PRN, rss_feed([rss_item("n1", "a", "b", "https://x.test/1"),
                                             rss_item("old-1", "Old news", "x", "https://x.test/o")]))
                    await radar.poll_wire(PRN)            # overlap with the last poll: fine
                    world.set(PRN, rss_feed([rss_item("n2", "a", "b", "https://x.test/2"),
                                             rss_item("n3", "a", "b", "https://x.test/3")]))
                    await radar.poll_wire(PRN)            # nothing known left: gap
                    return radar
            radar = run(scenario())
        entry = radar.state.sources["PR Newswire"]
        self.assertEqual(entry["overflows"], 1)
        self.assertTrue(entry["ok"])
        self.assertIn("ייתכן שפוספסו ידיעות 1 פעמים", radar.status_text())

    def test_prn_category_feed_names(self) -> None:
        self.assertEqual(bot.wire_source_name(PRN), "PR Newswire")
        self.assertEqual(bot.wire_source_name(
            "https://www.prnewswire.com/rss/health-latest-news/biotechnology-list.rss"),
            "PR Newswire · biotechnology")
        self.assertEqual(bot.wire_source_name(
            "https://www.prnewswire.com/rss/health-latest-news/health-latest-news-list.rss"),
            "PR Newswire · health")

    def test_same_release_in_two_feeds_is_processed_once(self) -> None:
        bio = "https://www.prnewswire.com/rss/health-latest-news/biotechnology-list.rss"
        world = base_world()
        world.set(bio, rss_feed([rss_item("old-bio", "Old", "x", "https://x.test/ob")]))
        with tempfile.TemporaryDirectory() as d, mock.patch.object(bot, "SEC_MIN_INTERVAL", 0.0):
            async def scenario() -> None:
                async with world.client() as client:
                    radar = bot.Radar(make_cfg(Path(d), wire_feeds=[PRN, bio]), client)
                    await radar.refresh_tickers()
                    for u in (PRN, bio):
                        await radar.poll_wire(u)
                    item = rss_item("https://www.prnewswire.com/oklo.html",
                                    "Oklo Awarded $450 Million Contract by U.S. Department of Defense",
                                    "Oklo Inc. (NYSE: OKLO) today announced", "https://www.prnewswire.com/oklo.html")
                    world.set(PRN, rss_feed([item]))
                    world.set(bio, rss_feed([item]))
                    world.set("https://www.prnewswire.com/oklo.html", CONTRACT_PR)
                    await asyncio.gather(radar.poll_wire(PRN), radar.poll_wire(bio))
                    await radar.drain()
            run(scenario())
        self.assertEqual(len(world.sent), 1)

    def test_ticker_from_company_name_in_headline(self) -> None:
        tm = bot.TickerMap()
        tm.load({"fields": ["cik", "name", "ticker", "exchange"], "data": [
            [1, "Kirby Corp", "KEX", "NYSE"], [2, "Apple Inc.", "AAPL", "Nasdaq"],
            [3, "Apple Hospitality REIT, Inc.", "APLE", "NYSE"], [4, "Global Industries Inc", "GLBL", "NYSE"],
            [5, "Pink Co", "PINK", "OTC"]]})
        self.assertEqual(tm.match_title("Kirby Corporation Announces Third Quarter Date"), "KEX")
        self.assertEqual(tm.match_title("Apple Hospitality REIT Declares Dividend"), "APLE")
        self.assertEqual(tm.match_title("Apple Inc. Unveils New iPhone"), "AAPL")
        self.assertIsNone(tm.match_title("Global Payments Reports Results"))
        self.assertIsNone(tm.match_title("Pink Co Announces Deal"))  # OTC is not in the map

    def test_business_wire_item_without_ticker_uses_company_name(self) -> None:
        bw = "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtRWA=="
        world = base_world()
        world.set(bw, rss_feed([rss_item("bw-old", "Old", "x", "https://www.businesswire.com/news/home/old")]))
        calls: list[str] = []
        real = world.handler

        def spy(request: httpx.Request) -> httpx.Response:
            calls.append(str(request.url))
            return real(request)

        world.handler = spy  # type: ignore[method-assign]
        with tempfile.TemporaryDirectory() as d:
            async def scenario() -> None:
                async with world.client() as client:
                    radar = bot.Radar(make_cfg(Path(d), wire_feeds=[bw]), client)
                    await radar.refresh_tickers()
                    await radar.poll_wire(bw)
                    world.set(bw, rss_feed([rss_item(
                        "bw-1", "Oklo Inc. Awarded $450 Million Contract by U.S. Department of Defense",
                        "No ticker in this summary", "https://www.businesswire.com/news/home/oklo")]))
                    await radar.poll_wire(bw)
                    await radar.drain()
            run(scenario())
        self.assertEqual(len(world.sent), 1)
        self.assertIn("<b>OKLO</b> | Oklo Inc.", world.sent[0]["text"])
        self.assertIn("📰 Business Wire", world.sent[0]["text"])
        self.assertFalse(any("businesswire.com/news/home/oklo" in u for u in calls))  # page not fetched

    def test_wire_timeout_error_is_readable(self) -> None:
        world = base_world()

        def timeout(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("", request=request)

        world.routes[PRN] = timeout
        with tempfile.TemporaryDirectory() as d, mock.patch.object(asyncio, "sleep", new=mock.AsyncMock()):
            async def scenario() -> bot.Radar:
                async with world.client() as client:
                    radar = bot.Radar(make_cfg(Path(d)), client)
                    await radar.poll_wire(PRN)
                    return radar
            radar = run(scenario())
        self.assertEqual(radar.state.sources["PR Newswire"]["error"], "timeout (ReadTimeout)")

    def test_once_fails_on_rejected_token(self) -> None:
        world = base_world()
        real = world.telegram

        def reject(request: httpx.Request) -> httpx.Response:
            if str(request.url).endswith("/getUpdates"):
                return httpx.Response(401, json={"ok": False, "error_code": 401, "description": "Unauthorized"})
            return real(request)

        world.telegram = reject  # type: ignore[method-assign]
        with tempfile.TemporaryDirectory() as d:
            async def scenario() -> int:
                async with world.client() as client:
                    return await bot.Radar(make_cfg(Path(d)), client, once=True).run_once()
            self.assertEqual(run(scenario()), 1)


class DemoTest(unittest.TestCase):
    def test_demo_scores_real_items_and_leaves_state_alone(self) -> None:
        with tempfile.TemporaryDirectory() as d, mock.patch.object(bot, "SEC_MIN_INTERVAL", 0.0):
            tmp = Path(d)
            ctx = type("Ctx", (), {"world": base_world()})()
            PipelineTest.add_new_items(ctx)  # type: ignore[arg-type]
            world = ctx.world

            async def scenario() -> int:
                async with world.client() as client:
                    return await bot.Radar(make_cfg(tmp), client, once=True).run_demo()

            self.assertEqual(run(scenario()), 0)
            self.assertFalse((tmp / "state.json").exists())
        self.assertEqual(len(world.sent), 2)
        best, summary = world.sent[0]["text"], world.sent[1]["text"]
        self.assertIn("הדגמה עם ידיעה אמיתית", best)
        self.assertIn("הייתה נשלחת כהתראה", best)
        self.assertIn("<b>OKLO</b>", best)
        self.assertIn("❌ נפסל: הנפקה ודילול", summary)  # DLUT offering
        self.assertIn("TBIO", summary)                    # conference, +0


KOD_TEASER = ("Kodiak Sciences to Present Topline Results on September 28, 2026 from DAYBREAK Pivotal "
              "Phase 3 Study of Zenkuda and KSI-501 in Patients with Wet Age-Related Macular Degeneration")


class CatalystTest(unittest.TestCase):
    TODAY = __import__("datetime").date(2026, 9, 23)

    def test_detects_kodiak_style_teaser(self) -> None:
        cat = bot.find_catalyst(KOD_TEASER, self.TODAY)
        self.assertIsNotNone(cat)
        self.assertEqual((cat.label, cat.date.isoformat()), ("תוצאות ניסוי קליני", "2026-09-28"))

    def test_detects_other_catalysts(self) -> None:
        cases = {
            "Acme (NASDAQ: ACME) will host a conference call on October 5 at 8:00 a.m. ET to discuss "
            "topline data from its Phase 3 ALPHA trial.": "2026-10-05",
            "FDA accepts NDA; PDUFA target action date set for March 15, 2027": "2027-03-15",
            "FDA Advisory Committee meeting scheduled for November 12 to review the BLA": "2026-11-12",
            "Acme Inc. to Host Conference Call Today at 8:00 a.m. ET to Discuss Topline Phase 3 Results":
                "2026-09-23",
        }
        for text, expected in cases.items():
            cat = bot.find_catalyst(text, self.TODAY)
            self.assertIsNotNone(cat, text)
            self.assertEqual(cat.date.isoformat(), expected, text)

    def test_ignores_non_catalysts(self) -> None:
        for text in (
            "Acme to report second quarter financial results on October 30, 2026",      # earnings date
            "Acme expects to report topline data from the Phase 3 trial in 2H 2027",    # no concrete date
            "Acme will present topline results from Phase 2 study on September 1, 2026",  # already past
            "SAN DIEGO, Sept. 21, 2026 -- Acme announced a new CFO.",                  # dateline only
        ):
            self.assertIsNone(bot.find_catalyst(text, self.TODAY), text)

    def run_teaser(self, world: "World", tmp: Path) -> bot.Radar:
        world.set(PRN, rss_feed([rss_item(
            "kod-1", KOD_TEASER, "Kodiak Sciences Inc. (NASDAQ: KOD) today announced",
            "https://www.prnewswire.com/kod.html")]))
        world.set("https://www.prnewswire.com/kod.html",
                  f"<html><div class='release-body'><p>{KOD_TEASER}.</p></div></html>")
        eastern = __import__("datetime").datetime(2026, 9, 23, 9, 0)

        async def scenario() -> bot.Radar:
            async with world.client() as client:
                radar = bot.Radar(make_cfg(tmp), client)
                radar.state.initialized.add(f"wire:{PRN}")  # not a first run
                await radar.poll_wire(PRN)
                await radar.drain()
                await radar.poll_wire(PRN)  # same item again: no second heads-up
                await radar.drain()
                return radar

        with mock.patch.object(bot, "us_eastern_now", return_value=eastern):
            return run(scenario())

    def test_teaser_sends_heads_up_and_watches_ticker(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            world = base_world()
            radar = self.run_teaser(world, Path(d))
        self.assertEqual(len(world.sent), 1)
        msg = world.sent[0]["text"]
        self.assertIn("📅 <b>קטליזטור צפוי</b>", msg)
        self.assertIn("<b>KOD</b>", msg)
        self.assertIn("28 בספטמבר 2026 (בעוד 5 ימים)", msg)
        self.assertIn("➕ נוספה לרשימת המעקב", msg)
        self.assertIn("KOD", radar.state.watchlist)
        self.assertIn("KOD:2026-09-28", radar.state.catalysts)
        with mock.patch.object(bot, "us_eastern_now",
                               return_value=__import__("datetime").datetime(2026, 9, 23, 9, 0)):
            self.assertIn("KOD", radar.catalysts_text())

    def test_reminder_on_the_day_once(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            world = base_world()
            radar = self.run_teaser(world, Path(d))
            world.sent.clear()
            dtm = __import__("datetime")

            async def remind(at: object) -> None:
                with mock.patch.object(bot, "us_eastern_now", return_value=at):
                    await radar.check_catalyst_reminders()

            async def scenario() -> None:
                async with world.client() as client:
                    radar.tg.client = client
                    await remind(dtm.datetime(2026, 9, 27, 12, 0))  # day before: nothing
                    await remind(dtm.datetime(2026, 9, 28, 3, 0))   # before pre-market: nothing
                    await remind(dtm.datetime(2026, 9, 28, 4, 5))   # reminder
                    await remind(dtm.datetime(2026, 9, 28, 8, 0))   # only once

            run(scenario())
        self.assertEqual(len(world.sent), 1)
        self.assertIn("⏰ <b>היום: קטליזטור צפוי</b>", world.sent[0]["text"])
        self.assertIn("KOD", world.sent[0]["text"])

    def test_catalysts_survive_restart(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            radar = self.run_teaser(base_world(), Path(d))
            with mock.patch.object(bot, "us_eastern_now",
                                   return_value=__import__("datetime").datetime(2026, 9, 23, 9, 0)):
                radar.state.save()
            st = bot.State.load(Path(d) / "state.json", [])
        self.assertIn("KOD:2026-09-28", st.catalysts)


class PumpRiskTest(unittest.TestCase):
    TODAY = __import__("datetime").date(2026, 9, 29)

    @staticmethod
    def submissions(rows: list[tuple[str, str, str]]) -> dict[str, Any]:
        return {"filings": {"recent": {"form": [r[0] for r in rows], "filingDate": [r[1] for r in rows],
                                       "items": [r[2] for r in rows]}}}

    def test_dilution_and_delisting_history_is_high_risk(self) -> None:
        subs = self.submissions([
            ("424B5", "2026-08-01", ""), ("424B5", "2026-06-15", ""), ("S-3", "2026-01-10", ""),
            ("8-K", "2026-05-02", "3.01,9.01"), ("10-Q", "2026-08-10", ""),
        ])
        risk = bot.assess_pump_risk("Company signs non-binding MOU with partner", subs, self.TODAY)
        self.assertEqual(risk.level, "high")
        text = " ".join(risk.reasons)
        self.assertIn("424B5", text)
        self.assertIn("3.01", text)
        self.assertIn("MOU", text)

    def test_large_company_debt_shelf_is_not_a_pump_sign(self) -> None:
        rows = [("424B2", "2026-08-01", ""), ("424B2", "2026-07-01", ""), ("S-3ASR", "2026-02-01", "")]
        subs = {**self.submissions(rows), "category": "Large accelerated filer"}
        risk = bot.assess_pump_risk("L3Harris Receives Contract Valued at up to $6 Billion", subs, self.TODAY)
        self.assertEqual(risk.level, "")
        small = {**self.submissions(rows), "category": "Non-accelerated filer<br>Smaller reporting company"}
        self.assertIn(bot.assess_pump_risk("x", small, self.TODAY).level, ("medium", "high"))
        # distress still counts for a large filer
        distress = {**self.submissions(rows + [("8-K", "2026-05-02", "3.01"), ("NT 10-Q", "2026-05-15", "")]),
                    "category": "Large accelerated filer"}
        self.assertEqual(bot.assess_pump_risk("x", distress, self.TODAY).level, "medium")

    def test_clean_company_has_no_warning(self) -> None:
        subs = self.submissions([("10-Q", "2026-08-10", ""), ("8-K", "2026-07-01", "2.02,9.01"),
                                 ("424B5", "2024-01-01", "")])  # old offering: out of the window
        risk = bot.assess_pump_risk("Company awarded $450 million government contract", subs, self.TODAY)
        self.assertEqual((risk.level, risk.reasons), ("", []))

    def test_text_alone_can_raise_medium(self) -> None:
        risk = bot.assess_pump_risk("Signs letter of intent for bitcoin treasury strategy", None, self.TODAY)
        self.assertEqual(risk.level, "medium")

    def test_alert_carries_pump_warning(self) -> None:
        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            w = base_world()
            w.set("https://data.sec.gov/submissions/CIK0001849056.json", self.submissions([
                ("424B5", "2026-09-01", ""), ("8-K", "2026-04-01", "3.01"),
            ]))
            with mock.patch.object(bot, "SEC_MIN_INTERVAL", 0.0), \
                    mock.patch.object(bot, "us_eastern_now",
                                      return_value=__import__("datetime").datetime(2026, 9, 29, 9, 0)):
                async with w.client() as client:
                    radar = bot.Radar(make_cfg(Path(tmp.name)), client)
                    await radar.refresh_tickers()
                    c = bot.Candidate(**{**bot.SAMPLE_CANDIDATE.__dict__, "published_ts": time.time()})
                    risk = await radar.pump_risk(c)
            self.assertEqual(risk.level, "high")
            msg = bot.format_alert(c, 5, "חוזה", risk=risk)
            self.assertIn("🔴 סיכון פמפום גבוה", msg)
            self.assertIn("424B5", msg)

        run(scenario())

    def test_sec_failure_never_blocks_alert(self) -> None:
        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            w = base_world()  # no submissions route -> 404
            async with w.client() as client:
                radar = bot.Radar(make_cfg(Path(tmp.name)), client)
                await radar.refresh_tickers()
                c = bot.Candidate(**{**bot.SAMPLE_CANDIDATE.__dict__})
                risk = await radar.pump_risk(c)
            self.assertEqual(risk.level, "")
            self.assertNotIn("פמפום", bot.format_alert(c, 5, "x", risk=risk))

        run(scenario())


class ContinuousRunTest(unittest.TestCase):
    def test_timed_runs_end_at_a_quiet_minute(self) -> None:
        dtm = __import__("datetime")
        tz = bot.eastern_tz()
        for start_min in (0, 7, 14, 29, 31, 50):
            start = dtm.datetime(2026, 9, 29, 7, start_min, 30, tzinfo=tz).timestamp()
            d = bot.quiet_stop(start, 3600)
            end = dtm.datetime.fromtimestamp(start + d, tz)
            self.assertIn(end.minute, (14, 44), (start_min, end))
            self.assertEqual(end.second, 0)
            self.assertTrue(1800 <= d <= 3600, d)
        self.assertEqual(bot.quiet_stop(0, 60), 60)   # short runs are left alone


    def test_run_for_stops_by_itself_and_alerts_quietly(self) -> None:
        cat_feed = "https://www.prnewswire.com/rss/health-latest-news/biotechnology-list.rss"

        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            w = base_world()
            w.set(cat_feed, rss_feed([rss_item("cat-1", "Old", "x", "https://www.prnewswire.com/c1.html")]))
            cfg = make_cfg(Path(tmp.name), wire_feeds=[PRN, cat_feed], wire_poll=0.05, edgar_poll=0.05)
            with mock.patch.object(bot, "SEC_MIN_INTERVAL", 0.0):
                async with w.client() as client:
                    radar = bot.Radar(cfg, client)

                    async def news_arrives() -> None:
                        await asyncio.sleep(0.3)
                        w.set(PRN, rss_feed([
                            rss_item("prn-2", "Oklo Awarded $450 Million Contract by U.S. Department of Defense",
                                     "Oklo Inc. (NYSE: OKLO) today announced",
                                     "https://www.prnewswire.com/oklo.html"),
                            rss_item("old-1", "Old news", "Oklo Inc. (NYSE: OKLO) old",
                                     "https://www.prnewswire.com/old-1.html"),
                        ]))
                        w.set("https://www.prnewswire.com/oklo.html", CONTRACT_PR)

                    started = time.monotonic()
                    await asyncio.gather(radar.run(duration=1.0), news_arrives())
                    self.assertLess(time.monotonic() - started, 5)
            texts = [m["text"] for m in w.sent]
            self.assertFalse(any("פעיל" in t for t in texts), texts)  # no start-up message
            self.assertEqual(len(texts), 1, texts)
            self.assertIn("OKLO", texts[0])
            self.assertIn("OKLO", bot.State.load(cfg.state_file, []).last_alert)
            polls = [r for r in w.requests if str(r.url) == PRN]
            cat_polls = [r for r in w.requests if str(r.url) == cat_feed]
            self.assertGreater(len(polls), len(cat_polls) * 2)  # category feeds polled less often
            self.assertGreaterEqual(len(cat_polls), 1)

        run(scenario())


def yahoo_chart(start: float, prices: list[float]) -> dict[str, Any]:
    ts = [int(start + 60 * i) for i in range(len(prices))]
    opens = [prices[0]] + prices[:-1]
    return {"chart": {"result": [{"timestamp": ts, "indicators": {"quote": [{
        "open": opens, "close": prices, "high": [max(o, c) for o, c in zip(opens, prices)],
        "low": [min(o, c) for o, c in zip(opens, prices)], "volume": [1000] * len(prices)}]}}]}}


class PerformanceTest(unittest.TestCase):
    def test_alert_is_logged_and_daily_report_measures_it(self) -> None:
        dtm = __import__("datetime")
        tz = bot.eastern_tz()
        alert_ts = dtm.datetime(2026, 9, 28, 10, 0, tzinfo=tz).timestamp()
        bars_from = alert_ts - 600
        # flat until 10:05, then +0.5% per minute
        prices = [10.0] * 10 + [10.0 * (1 + 0.005 * i) for i in range(1, 360)]

        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            w = base_world()
            w.set("https://query1.finance.yahoo.com/v8/finance/chart/OKLO?*", yahoo_chart(bars_from, prices))
            cfg = make_cfg(Path(tmp.name), positive_only=False, pump_check=False)
            async with w.client() as client:
                radar = bot.Radar(cfg, client)
                c = bot.Candidate(**{**bot.SAMPLE_CANDIDATE.__dict__, "watch": True})
                await radar.process(c)
                self.assertEqual(len(radar.state.alert_log), 1)
                self.assertEqual(radar.state.alert_log[0]["ticker"], "OKLO")
                radar.state.alert_log[0]["t"] = alert_ts   # pretend it went out on Monday 10:00
                radar.state.alert_log[0]["score"] = 5
                w.sent.clear()
                before = dtm.datetime(2026, 9, 28, 19, 0, tzinfo=tz)
                with mock.patch.object(bot, "us_eastern_now", return_value=before):
                    await radar.check_performance_report()
                self.assertEqual(w.sent, [])                 # not before 20:10
                after = dtm.datetime(2026, 9, 28, 20, 15, tzinfo=tz)
                with mock.patch.object(bot, "us_eastern_now", return_value=after):
                    await radar.check_performance_report()
                    await radar.check_performance_report()   # once a day
                self.assertEqual(len(w.sent), 2, [m["text"] for m in w.sent])   # trades + running total
                self.assertIn("מצטבר", w.sent[1]["text"])
                text = w.sent[0]["text"]
                self.assertIn("דוח ביצועים יומי", text)
                self.assertIn("<b>OKLO</b> (+5)", text)
                self.assertIn("אחרי 3 דק", text)
                r = radar.state.alert_log[0]["r"]
                self.assertEqual(r["entry_delay_s"], 180)
                self.assertGreater(r["r_5m"], 0.02)
                self.assertTrue(r["tradable"])
                self.assertIn("מצטבר", bot.perf_summary_text(radar.state.alert_log))
                radar.state.save()
                self.assertEqual(bot.State.load(cfg.state_file, []).alert_log[0]["r"]["entry_delay_s"], 180)

        run(scenario())

    def test_alert_log_is_seeded_from_last_alerts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            path.write_text(json.dumps({"watchlist": [], "last_alert": {"NVTS": 100.0, "KOD": 50.0}}))
            st = bot.State.load(path, [])
            self.assertEqual([e["ticker"] for e in st.alert_log], ["KOD", "NVTS"])
            st.save()
            self.assertEqual(len(bot.State.load(path, []).alert_log), 2)   # seeded only once

    def test_long_report_is_split(self) -> None:
        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            w = base_world()
            async with w.client() as client:
                radar = bot.Radar(make_cfg(Path(tmp.name)), client)
                fresh = [{"t": 0, "ticker": f"T{i}", "score": 4, "pump": "none", "r": {
                    "entry": 1.0, "entry_delay_s": 180, "tradable": True, "r_5m": 0.01, "r_30m": 0.02,
                    "r_close": 0.03, "r_60m": 0.0, "tp2_sl2": 0.02, "session": "regular", "pre_move": 0.0}}
                         for i in range(60)]
                radar.state.alert_log = fresh
                dtm = __import__("datetime")
                with mock.patch.object(radar, "measure_alerts", return_value=fresh), \
                        mock.patch.object(bot, "us_eastern_now",
                                          return_value=dtm.datetime(2026, 9, 28, 20, 15, tzinfo=bot.eastern_tz())):
                    await radar.check_performance_report()
            self.assertGreater(len(w.sent), 2)
            self.assertTrue(all(len(m["text"]) < 4096 for m in w.sent))
            self.assertIn("60 עסקאות", w.sent[-1]["text"])

        run(scenario())

    def test_perf_command_without_data(self) -> None:
        self.assertIn("עדיין אין", bot.perf_summary_text([]))


INFO_NS = "http://www.sec.gov/edgar/document/thirteenf/informationtable"


def info_table(rows: list[tuple[str, str, int, int, str]]) -> str:
    """rows: (issuer, cusip, value $, shares, putCall)"""
    body = "".join(
        f"<infoTable><nameOfIssuer>{n}</nameOfIssuer><titleOfClass>COM</titleOfClass><cusip>{c}</cusip>"
        f"<value>{v}</value><shrsOrPrnAmt><sshPrnamt>{sh}</sshPrnamt><sshPrnamtType>SH</sshPrnamtType>"
        f"</shrsOrPrnAmt>{f'<putCall>{pc}</putCall>' if pc else ''}<investmentDiscretion>SOLE</investmentDiscretion>"
        f"</infoTable>" for n, c, v, sh, pc in rows)
    return f'<?xml version="1.0" encoding="UTF-8"?><informationTable xmlns="{INFO_NS}">{body}</informationTable>'


Q1 = [("APPLE INC", "037833100", 60_000_000_000, 300_000_000, ""),
      ("BANK AMER CORP", "060505104", 30_000_000_000, 700_000_000, ""),
      ("KRAFT HEINZ CO", "500754106", 10_000_000_000, 325_000_000, ""),
      ("CHEVRON CORP NEW", "166764100", 5_000_000_000, 30_000_000, "")]
Q2 = [("APPLE INC", "037833100", 50_000_000_000, 250_000_000, ""),           # -17%
      ("BANK AMER CORP", "060505104", 20_000_000_000, 400_000_000, ""),      # with the next line: -40%
      ("BANK AMER CORP", "060505104", 1_000_000_000, 20_000_000, ""),        # second line, merged
      ("KRAFT HEINZ CO", "500754106", 10_500_000_000, 325_000_000, ""),     # unchanged
      ("CONSTELLATION BRANDS INC", "21036P108", 1_200_000_000, 5_600_000, ""),  # new
      ("ALPHABET INC", "02079K305", 900_000_000, 5_000_000, "Call")]         # new option
# Chevron sold out


class GuruTest(unittest.TestCase):
    def test_parse_and_diff(self) -> None:
        cur = bot.parse_13f_table(info_table(Q2))
        self.assertEqual(cur["060505104"].shares, 420_000_000)        # lines merged
        self.assertIn("02079K305:Call", cur)
        changes = bot.diff_13f(bot.parse_13f_table(info_table(Q1)), cur)
        self.assertEqual([h.cusip for _, h, _ in changes["new"]], ["21036P108", "02079K305"])
        self.assertEqual([p.cusip for p, _, _ in changes["sold"]], ["166764100"])
        self.assertEqual({p.cusip for p, _, _ in changes["reduced"]}, {"037833100", "060505104"})
        self.assertEqual(changes["added"], [])

    def test_values_reported_in_thousands_are_scaled(self) -> None:
        rows = [(n, c, v // 1000, sh, pc) for n, c, v, sh, pc in Q1]
        cur = bot.parse_13f_table(info_table(rows))
        self.assertEqual(cur["037833100"].value, 60_000_000_000)
        self.assertEqual(bot.parse_13f_table(info_table(Q1))["037833100"].value, 60_000_000_000)

    def test_new_filing_is_reported_once(self) -> None:
        cik = 1067983
        acc2, acc1 = "0000950123-26-008100", "0000950123-26-005000"
        subs = {"filings": {"recent": {
            "form": ["4", "13F-HR", "SC 13G", "13F-HR"],
            "accessionNumber": ["x", acc2, "y", acc1],
            "filingDate": ["2026-08-20", "2026-08-14", "2026-08-01", "2026-05-15"],
            "reportDate": ["2026-08-19", "2026-06-30", "", "2026-03-31"]}}}
        figi_calls = []

        def figi(request: httpx.Request) -> httpx.Response:
            jobs = json.loads(request.content)
            figi_calls.append(jobs)
            known = {"037833100": "AAPL", "060505104": "BAC", "21036P108": "STZ", "166764100": "CVX"}
            return httpx.Response(200, json=[{"data": [{"ticker": known[j["idValue"]]}]} if j["idValue"] in known
                                             else {"warning": "No identifier found."} for j in jobs])

        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            w = base_world()
            w.set(f"https://data.sec.gov/submissions/CIK{cik:010d}.json", subs)
            for acc, rows in ((acc2, Q2), (acc1, Q1)):
                base = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-', '')}"
                w.set(f"{base}/index.json", {"directory": {"item": [
                    {"name": "primary_doc.xml"}, {"name": "50240.xml"}, {"name": f"{acc}-index.htm"}]}})
                w.set(f"{base}/50240.xml", info_table(rows))
            w.routes[bot.OPENFIGI_URL] = figi
            cfg = make_cfg(Path(tmp.name), gurus=[(cik, "Warren Buffett", "Berkshire Hathaway")])
            with mock.patch.object(bot, "SEC_MIN_INTERVAL", 0.0):
                async with w.client() as client:
                    radar = bot.Radar(cfg, client)
                    await radar.refresh_tickers()
                    await radar.check_gurus()
                    self.assertEqual(len(w.sent), 1, [m["text"] for m in w.sent])
                    text = w.sent[0]["text"]
                    for part in ("Warren Buffett", "Q2 2026", "קנו (פוזיציה חדשה)", "<b>STZ</b>", "(Call)",
                                 "הקטינו", "<b>BAC</b>", "-40%", "מכרו הכול", "<b>CVX</b>", "GuruFocus",
                                 "gurufocus.com/guru/top-holdings", "דיווח"):
                        self.assertIn(part, text)
                    self.assertNotIn("Kraft", text)                            # unchanged position
                    self.assertEqual(radar.state.gurus[str(cik)], acc2)
                    # no new filing: nothing sent, even when the 30-minute gate opens again
                    radar.state.guru_checked = 0
                    await radar.check_gurus()
                    self.assertEqual(len(w.sent), 1)
                    self.assertEqual(len(figi_calls), 1)                    # tickers cached
                    self.assertIn("✅ Warren Buffett", radar.gurus_text())

        run(scenario())


class GainersStudyTest(unittest.TestCase):
    def test_helpers(self) -> None:
        dtm = __import__("datetime")
        tz = bot.eastern_tz()
        day = dtm.date(2026, 10, 7)
        t = lambda d: int(dtm.datetime(2026, 10, d, 9, 30, tzinfo=tz).timestamp())  # noqa: E731
        spark = {"SXTC": {"timestamp": [t(5), t(6), t(7)], "close": [1.2, 1.25, 2.83]},
                 "OLD": {"timestamp": [t(5), t(6)], "close": [1.0, 2.0]},          # no bar today
                 "AAPL": {"timestamp": [t(6), t(7)], "close": [100.0, 101.0]}}
        moves = bot.spark_moves(spark, day)
        self.assertAlmostEqual(moves["SXTC"][0], 126.4)
        self.assertNotIn("OLD", moves)
        self.assertAlmostEqual(moves["AAPL"][0], 1.0)
        tm = bot.TickerMap()
        tm.load({"fields": ["cik", "name", "ticker", "exchange"],
                 "data": [[1, "A", "ABCD", "Nasdaq"], [2, "B", "FUSEW", "Nasdaq"], [3, "C", "BRK-B", "NYSE"]]})
        self.assertEqual(bot.research_universe(tm), ["ABCD", "BRK-B"])
        self.assertEqual(bot.classify_catalyst("Acme Receives FDA Approval for X"), "FDA / רגולציה")
        self.assertEqual(bot.classify_catalyst("Acme Awarded $50M Army Contract"), "חוזה / הזמנה")
        self.assertEqual(bot.classify_catalyst("Acme to Be Acquired by Big Co for $5 per Share"), "מיזוג / רכישה")
        self.assertEqual(bot.classify_catalyst(""), bot.NO_NEWS)
        prof = bot.move_profile([(100.0, 1.0), (160.0, 1.05), (220.0, 1.12), (280.0, 1.5), (340.0, 1.3)], 1.0)
        self.assertEqual(prof["start"], 220.0)
        self.assertAlmostEqual(prof["peak_pct"], 50.0)
        news = {"news": [{"title": "Acme wins", "publisher": "ACCESS Newswire", "providerPublishTime": 5,
                          "relatedTickers": ["ACME"]},
                         {"title": "Other co", "publisher": "X", "providerPublishTime": 6, "relatedTickers": ["ZZZ"]}]}
        self.assertEqual(bot.yahoo_news_items(news, "ACME"), [{"pub": 5.0, "src": "ACCESS Newswire", "title": "Acme wins"}])
        self.assertTrue(bot.is_our_source("GlobeNewswire"))
        self.assertFalse(bot.is_our_source("ACCESS Newswire"))

    def test_daily_study_report_and_learning(self) -> None:
        dtm = __import__("datetime")
        tz = bot.eastern_tz()
        at = lambda h, m: dtm.datetime(2026, 10, 7, h, m, tzinfo=tz).timestamp()  # noqa: E731
        y = lambda: at(9, 30) - 86400  # noqa: E731
        spark = {"OKLO": {"timestamp": [y(), at(9, 30)], "close": [5.0, 9.0]},
                 "TEVA": {"timestamp": [y(), at(9, 30)], "close": [15.4, 20.0]},
                 "AAPL": {"timestamp": [y(), at(9, 30)], "close": [100.0, 101.0]}}

        def chart(prev: float, start: float) -> dict:
            ts = [int(at(4, 0)) + 60 * i for i in range(900)]
            px = [prev * (1.0 if t < start else 1.5) for t in ts]
            return {"chart": {"result": [{"timestamp": ts, "indicators": {"quote": [{
                "open": px, "high": px, "low": px, "close": px, "volume": [5000] * len(ts)}]}}]}}

        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            w = base_world()
            w.routes["https://query1.finance.yahoo.com/v8/finance/spark*"] = (200, spark)
            w.routes["https://query1.finance.yahoo.com/v8/finance/chart/OKLO?*"] = (200, chart(5.0, at(8, 5)))
            w.routes["https://query1.finance.yahoo.com/v8/finance/chart/TEVA?*"] = (200, chart(15.4, at(10, 30)))
            w.routes["https://query1.finance.yahoo.com/v1/finance/search?q=OKLO*"] = (200, {"news": []})
            w.routes["https://query1.finance.yahoo.com/v1/finance/search?q=TEVA*"] = (200, {"news": [
                {"title": "Teva Announces Partnership With Big Pharma", "publisher": "ACCESS Newswire",
                 "providerPublishTime": int(at(10, 0)), "relatedTickers": ["TEVA"]}]})
            cfg = make_cfg(Path(tmp.name))
            async with w.client() as client:
                radar = bot.Radar(cfg, client)
                await radar.refresh_tickers()
                radar.tickers._add(9, "Apple Inc.", "AAPL")
                radar.state.news_log["OKLO"] = [{"t": at(8, 0) + 20, "pub": at(8, 0), "src": "PR Newswire",
                                                 "title": "Oklo Awarded $450 Million Contract by U.S. Department of Defense"}]
                radar.state.alert_log.append({"t": at(8, 1), "ticker": "OKLO", "score": 5, "pump": "none"})
                radar.state.news_log_since = at(0, 0) - 86400
                with mock.patch.object(bot, "us_eastern_now", return_value=dtm.datetime(2026, 10, 7, 20, 25, tzinfo=tz)), \
                        mock.patch.object(bot.asyncio, "sleep", new=mock.AsyncMock()):
                    await radar.check_gainers_study()
                    await radar.check_gainers_study()          # once a day
            text = "\n".join(m["text"] for m in w.sent)
            self.assertIn("המזנקות של היום", text)
            self.assertIn("<b>OKLO</b> +80%", text)
            self.assertIn("הזינוק התחיל 08:05", text)
            self.assertIn("חוזה / הזמנה · PR Newswire 08:00 (5 דק' לפני הזינוק)", text)
            self.assertIn("הבוט התריע 4 דק' לפני הזינוק ✅", text)
            self.assertIn("ACCESS Newswire 10:00 (30 דק' לפני הזינוק) · מקור שהבוט לא קורא", text)
            self.assertNotIn("AAPL", text)                                # +1% is not a gainer
            log_ = radar.state.gainers_log
            self.assertEqual([e["ticker"] for e in log_], ["OKLO", "TEVA"])
            self.assertEqual(log_[1]["cat"], "שותפות / רישיון")
            self.assertTrue(log_[1]["missing_source"])
            summary = bot.learning_summary(log_)
            for part in ("מה למדתי", "עם חדשות שהבוט ראה: 1", "חוזה / הזמנה", "PR Newswire: 1 · 5 דק'",
                         "ACCESS Newswire: 1 · 30 דק' ⚠️ לא במעקב", "התריע לפני תחילת הזינוק: 1"):
                self.assertIn(part, summary)
            self.assertEqual(len(w.sent), 1)

        run(scenario())

    def test_no_study_on_the_first_partial_day(self) -> None:
        dtm = __import__("datetime")
        tz = bot.eastern_tz()

        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            w = base_world()
            async with w.client() as client:
                radar = bot.Radar(make_cfg(Path(tmp.name)), client)
                radar.state.news_log_since = dtm.datetime(2026, 10, 7, 17, 0, tzinfo=tz).timestamp()
                with mock.patch.object(bot, "us_eastern_now", return_value=dtm.datetime(2026, 10, 7, 20, 25, tzinfo=tz)):
                    await radar.check_gainers_study()
            self.assertEqual(w.sent, [])
            self.assertFalse(any("api.nasdaq.com" in str(r.url) for r in w.requests))

        run(scenario())

    def test_cheap_stock_boost(self) -> None:
        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            w = base_world()
            w.routes["https://query1.finance.yahoo.com/v8/finance/chart/OKLO?*"] = (
                200, {"chart": {"result": [{"meta": {"regularMarketPrice": 1.85}}]}})
            w.routes["https://query1.finance.yahoo.com/v8/finance/chart/TEVA?*"] = (
                200, {"chart": {"result": [{"meta": {"regularMarketPrice": 25.0}}]}})
            async with w.client() as client:
                radar = bot.Radar(make_cfg(Path(tmp.name)), client)
                cheap = bot.Candidate(source="wire", source_label="GlobeNewswire", ticker="OKLO", company="Oklo",
                                      title="Oklo Secures Follow-On Order From Utility", link="https://x/1",
                                      published_ts=None, watch=False, summary="Oklo secures order.")
                rich = bot.Candidate(**{**cheap.__dict__, "ticker": "TEVA", "company": "Teva", "link": "https://x/2"})
                s1, r1, _, _ = await radar.evaluate(cheap)
                s2, _, _, _ = await radar.evaluate(rich)
                await radar.evaluate(cheap)                                # price cached
            self.assertEqual((s1, s2), (4, 3))
            self.assertIn("מניה זולה ($1.85)", r1)
            self.assertEqual(sum(1 for r in w.requests if "/chart/OKLO" in str(r.url)), 1)

        run(scenario())

    def test_news_log_is_pruned_and_deduplicated(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            st = bot.State(Path(d) / "s.json")
            st.log_news("AAA", "PR Newswire", "Acme wins big contract", 1.0)
            st.log_news("AAA", "PR Newswire · technology", "Acme wins big contract", 1.0)
            self.assertEqual(len(st.news_log["AAA"]), 1)
            st.news_log["OLD"] = [{"t": 0, "pub": 0, "src": "x", "title": "old"}]
            self.assertNotIn("OLD", st.pruned_news_log())


class MomentumTest(unittest.TestCase):
    def test_due_tickers(self) -> None:
        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            w = base_world()
            async with w.client() as client:
                radar = bot.Radar(make_cfg(Path(tmp.name)), client)
                now = time.time()
                radar.state.news_log = {
                    "AAA": [{"t": now - 600, "pub": now - 600, "src": "GlobeNewswire", "title": "A"}],
                    "BBB": [{"t": now - 600, "pub": now - 600, "src": "SEC 8-K", "title": "8-K 8.01"}],
                    "CCC": [{"t": now - 30 * 3600, "pub": None, "src": "PR Newswire", "title": "old"}],
                    "DDD": [{"t": now - 5 * 3600, "pub": None, "src": "Business Wire", "title": "D"}],
                }
                self.assertEqual(sorted(radar.momentum_due(now)), ["AAA", "DDD"])
                radar.momentum_checked["AAA"] = now - 60          # checked a minute ago (fresh news: every 2 min)
                radar.momentum_checked["DDD"] = now - 300         # older news: every 10 minutes
                self.assertEqual(radar.momentum_due(now), [])
                radar.state.momentum["tickers"] = ["AAA"]
                self.assertEqual(radar.momentum_due(now + 1000), ["DDD"])

        run(scenario())

    def test_breakout_alert_once_with_the_news(self) -> None:
        dtm = __import__("datetime")
        tz = bot.eastern_tz()
        day = dtm.datetime.now(tz).date()
        while day.weekday() >= 5:
            day -= dtm.timedelta(days=1)

        def chart(prev: float, last: float, vol: int) -> dict:
            t0 = int(time.time()) - 3600
            ts = [t0 + 60 * i for i in range(30)]
            px = [prev * 1.02] * 10 + [last] * 20
            return {"chart": {"result": [{"meta": {"chartPreviousClose": prev}, "timestamp": ts, "indicators": {
                "quote": [{"open": px, "high": px, "low": px, "close": px, "volume": [vol] * 30}]}}]}}

        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            w = base_world()
            w.routes["https://query1.finance.yahoo.com/v8/finance/chart/OKLO?*"] = (200, chart(5.0, 6.2, 20_000))
            w.routes["https://query1.finance.yahoo.com/v8/finance/chart/TEVA?*"] = (200, chart(20.0, 21.0, 90_000))
            async with w.client() as client:
                radar = bot.Radar(make_cfg(Path(tmp.name)), client)
                await radar.refresh_tickers()
                now = time.time()
                radar.state.news_log = {
                    "OKLO": [{"t": now - 900, "pub": now - 900, "src": "PR Newswire",
                              "title": "Oklo Secures Follow-On Order From Utility"}],
                    "TEVA": [{"t": now - 900, "pub": now - 900, "src": "GlobeNewswire", "title": "Teva update"}]}
                et = dtm.datetime.combine(day, dtm.time(8, 20), tzinfo=tz)
                with mock.patch.object(bot, "us_eastern_now", return_value=et), \
                        mock.patch.object(bot.asyncio, "sleep", new=mock.AsyncMock()):
                    await radar.check_momentum()
                    radar.momentum_checked.clear()
                    await radar.check_momentum()              # once per ticker per day
            texts = [m["text"] for m in w.sent]
            self.assertEqual(len(texts), 1, texts)
            self.assertIn("זינוק בתהליך", texts[0])
            self.assertIn("<b>OKLO</b>", texts[0])
            self.assertIn("+24%", texts[0])
            self.assertIn("Oklo Secures Follow-On Order", texts[0])
            self.assertIn("חוזה / הזמנה", texts[0])
            self.assertEqual(radar.state.momentum["tickers"], ["OKLO"])

        run(scenario())

    def test_runners_from_a_month_of_closes(self) -> None:
        data = {"FLYE": {"close": [1.0, 1.61, 1.5, 1.4, 1.3]},          # +61% day, still cheap
                "BIG": {"close": [100.0, 140.0, 141.0]},               # too expensive
                "CALM": {"close": [2.0, 2.1, 2.2]},                    # never ran
                "SPLIT": {"close": [0.1, 5.0, 5.1]}}                   # reverse split artifact
        self.assertEqual(set(bot.spark_runners(data)), {"FLYE"})

    def test_premarket_runner_caught_at_the_start(self) -> None:
        dtm = __import__("datetime")
        tz = bot.eastern_tz()

        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            w = base_world()
            t0 = int(time.time()) - 600
            px = [2.0] * 5 + [2.25] * 5                       # +12.5%, started 5 minutes ago
            w.routes["https://query1.finance.yahoo.com/v8/finance/chart/OKLO?*"] = (200, {"chart": {"result": [{
                "meta": {"chartPreviousClose": 2.0}, "timestamp": [t0 + 60 * i for i in range(10)],
                "indicators": {"quote": [{"open": px, "high": px, "low": px, "close": px, "volume": [60_000] * 10}]}}]}})
            async with w.client() as client:
                radar = bot.Radar(make_cfg(Path(tmp.name)), client)
                await radar.refresh_tickers()
                et = dtm.datetime.now(tz)
                while et.weekday() >= 5:
                    et -= dtm.timedelta(days=1)
                et = et.replace(hour=7, minute=20)
                radar.state.runners = {"day": et.date().isoformat(), "tickers": ["OKLO"]}
                with mock.patch.object(bot, "us_eastern_now", return_value=et), \
                        mock.patch.object(bot, "session_of", return_value="pre"), \
                        mock.patch.object(bot.asyncio, "sleep", new=mock.AsyncMock()):
                    await radar.check_momentum()
            texts = [m["text"] for m in w.sent]
            self.assertEqual(len(texts), 1, texts)
            self.assertIn("תחילת זינוק", texts[0])
            self.assertIn("רצה חזק גם החודש", texts[0])
            self.assertIn("+12%", texts[0])

        run(scenario())

    def test_breakout_names_news_from_outside_the_bot_sources(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        radar = bot.Radar(make_cfg(Path(tmp.name)), mock.Mock())
        now = time.time()
        web = [{"pub": now - 1800, "src": "Business Wire",
                "title": "bioAffinity Technologies Announces Japanese Patent Allowance"}]
        text = radar.breakout_text("BIAF", 19, 7.33, 47e6, now - 1500, web)
        self.assertIn("Business Wire", text)
        self.assertIn("Japanese Patent Allowance", text)
        old = [{"pub": now - 2 * 86400, "src": "ACCESS Newswire", "title": "The OLB Group Launches Share Buyback"}]
        self.assertIn("לפני 2 ימים", radar.breakout_text("OLB", 44, 0.56, 51e6, now - 600, old))
        self.assertIn("אין שום ידיעה", radar.breakout_text("JZ", 45, 0.68, 9.5e6, now - 600, []))


class StateTest(unittest.TestCase):
    def test_seen_is_capped_and_atomic(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "sub" / "state.json"
            st = bot.State.load(path, ["NVDA"])
            with mock.patch.object(bot, "MAX_SEEN", 5):
                for i in range(8):
                    st.mark_seen(f"k{i}")
            self.assertEqual(list(st.seen), ["k3", "k4", "k5", "k6", "k7"])
            st.save()
            self.assertFalse((Path(d) / "sub" / "state.json.tmp").exists())
            st2 = bot.State.load(path, ["IGNORED"])
            self.assertEqual(st2.watchlist, ["NVDA"])
            self.assertEqual(list(st2.seen), ["k3", "k4", "k5", "k6", "k7"])

    def test_corrupt_state_starts_fresh(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "state.json"
            path.write_text("{not json")
            st = bot.State.load(path, ["OKLO"])
            self.assertEqual(st.watchlist, ["OKLO"])


if __name__ == "__main__":
    unittest.main()


class JumpModelTest(unittest.TestCase):
    MODEL = {"weights": {"bias": -3.0, "cap=מתחת ל-$50M": 1.0, "p=contract": 1.5, "amount=סכום גדול משווי החברה": 0.3},
             "vocab": ["contract"], "base_rate": 0.05, "promote_at": 0.3, "mute_below": 0.02}

    def world(self) -> World:
        w = base_world()
        day = 86400
        now = int(time.time())
        w.set(bot.YAHOO_DAILY_URL.format(symbol="OKLO"), {"chart": {"result": [{
            "meta": {"regularMarketPrice": 3.0},
            "timestamp": [now - 8 * day, now - 7 * day, now - 6 * day, now - 5 * day, now - 4 * day],
            "indicators": {"quote": [{"close": [2.0, 2.1, 2.2, 2.3, 2.5]}]}}]}})
        w.set(bot.SEC_SHARES_URL.format(cik=1849056), {"units": {"shares": [
            {"end": "2025-12-31", "val": 9_000_000}, {"end": "2026-06-30", "val": 10_000_000}]}})
        w.set("https://data.sec.gov/submissions/CIK0001849056.json", {"sic": "4911", "filings": {"recent": {}}})
        return w

    def test_odds_from_live_data_and_golden_alert(self) -> None:
        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            with mock.patch.object(bot, "SEC_MIN_INTERVAL", 0.0):
                async with self.world().client() as client:
                    radar = bot.Radar(make_cfg(Path(tmp.name)), client)
                    radar.jump_model = {**self.MODEL, "vocab_set": {"contract"}}
                    await radar.refresh_tickers()
                    c = bot.Candidate(**{**bot.SAMPLE_CANDIDATE.__dict__, "published_ts": time.time()})
                    odds = await radar.jump_odds(c, 2, None)
                    assert odds is not None
                    self.assertAlmostEqual(odds.probability, 1 / (1 + math.exp(0.2)), places=4)
                    self.assertTrue(odds.golden)
                    self.assertTrue(radar.should_alert(2, odds))           # promoted despite a low rule score
                    msg = bot.format_alert(c, 2, "", odds=odds)
                    self.assertIn("🏆 ידיעת זהב", msg)
                    self.assertIn("סיכוי היסטורי לקפיצה של 20%+: 45%", msg)
                    self.assertIn("„contract” בכותרת", msg)
                    self.assertIn("שווי שוק: מתחת ל-$50M", msg)
                    self.assertEqual(radar.prices["OKLO"][1:], (3.0, 0.25))

        run(scenario())

    def test_gate(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        radar = bot.Radar(make_cfg(Path(tmp.name)), mock.Mock())
        self.assertTrue(radar.should_alert(4, None))
        self.assertFalse(radar.should_alert(3, None))
        radar.jump_model = self.MODEL
        self.assertFalse(radar.should_alert(5, bot.JumpOdds(0.01, 0.05, [])))   # news that never moved stocks
        self.assertTrue(radar.should_alert(4, bot.JumpOdds(0.05, 0.05, [])))
        self.assertFalse(radar.should_alert(3, bot.JumpOdds(0.05, 0.05, [])))

    def test_model_file_round_trip(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        p = Path(tmp.name) / "m.json"
        p.write_text(json.dumps(self.MODEL), encoding="utf-8")
        self.assertEqual(bot.load_jump_model(p)["vocab_set"], {"contract"})
        self.assertIsNone(bot.load_jump_model(Path(tmp.name) / "missing.json"))


class TakeoverTargetTest(unittest.TestCase):
    def test_acquirer_release_alerts_on_the_target(self) -> None:
        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            async with base_world().client() as client:
                radar = bot.Radar(make_cfg(Path(tmp.name)), client)
                await radar.refresh_tickers()
                c = bot.Candidate(source="wire", source_label="PR Newswire", ticker="TEVA", company="Teva",
                                  title="Teva Agrees To Acquire Oklo Inc., Advancing Its Energy Strategy",
                                  link="https://www.prnewswire.com/x", published_ts=None, watch=False)
                t = radar.takeover_target(c)
                assert t is not None
                self.assertEqual((t.ticker, t.link), ("OKLO", c.link))
                self.assertGreaterEqual(bot.rule_score(t.title, t.company, strong_text=t.title, title=t.title).score, 5)
                self.assertIsNone(radar.takeover_target(t))                       # no loop
                for title in ("Teva to Acquire Assets of Oklo Inc.", "Teva Reports Results"):
                    self.assertIsNone(radar.takeover_target(bot.Candidate(**{**c.__dict__, "title": title})))

        run(scenario())


class HaltsTest(unittest.TestCase):
    @staticmethod
    def feed(items: list[tuple[str, str, float]]) -> str:
        tz = bot.eastern_tz()
        rows = "".join(
            f"<item><title>{sym}</title><ndaq:HaltDate>{dt_.strftime('%m/%d/%Y')}</ndaq:HaltDate>"
            f"<ndaq:HaltTime>{dt_.strftime('%H:%M:%S')}.183</ndaq:HaltTime><ndaq:IssueSymbol>{sym}</ndaq:IssueSymbol>"
            f"<ndaq:IssueName>{sym} Inc.</ndaq:IssueName><ndaq:ReasonCode>{code}</ndaq:ReasonCode></item>"
            for sym, code, ts in items for dt_ in [__import__("datetime").datetime.fromtimestamp(ts, tz)])
        return ('<?xml version="1.0"?><rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
                f"<channel><title>halts</title>{rows}</channel></rss>")

    def test_parse(self) -> None:
        now = time.time()
        h = bot.parse_halts(self.feed([("RZAI", "LUDP", now)]))
        self.assertEqual((h[0].ticker, h[0].code), ("RZAI", "LUDP"))
        self.assertAlmostEqual(h[0].ts, int(now), delta=1)

    def test_t1_and_limit_up_alerts_once(self) -> None:
        dtm = __import__("datetime")

        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            w = base_world()
            now = time.time()
            w.set(bot.HALTS_URL, self.feed([("OKLO", "T1", now - 60), ("TEVA", "LUDP", now - 30),
                                            ("TEVA", "T1", now - 3 * 3600)]))     # old: ignored
            t0 = int(now) - 600
            px = [20.0] * 5 + [24.0] * 5
            w.routes["https://query1.finance.yahoo.com/v8/finance/chart/TEVA?*"] = (200, {"chart": {"result": [{
                "meta": {"chartPreviousClose": 20.0}, "timestamp": [t0 + 60 * i for i in range(10)],
                "indicators": {"quote": [{"open": px, "high": px, "low": px, "close": px, "volume": [50_000] * 10}]}}]}})
            async with w.client() as client:
                radar = bot.Radar(make_cfg(Path(tmp.name)), client)
                await radar.refresh_tickers()
                et = dtm.datetime.now(bot.eastern_tz()).replace(hour=11)
                while et.weekday() >= 5:
                    et -= dtm.timedelta(days=1)
                with mock.patch.object(bot, "us_eastern_now", return_value=et):
                    await radar.check_halts()
                    await radar.check_halts()                       # nothing twice
            texts = [m["text"] for m in w.sent]
            self.assertEqual(len(texts), 2, texts)
            self.assertIn("חדשות מהותיות בדרך", texts[0])
            self.assertIn("<b>OKLO</b>", texts[0])
            self.assertIn("Limit Up", texts[1])
            self.assertIn("<b>TEVA</b>", texts[1])

        run(scenario())


PRN_LIST_PAGE = """<div class="row newsCards" lang="en-US"> <div role="group" aria-label="News Release" class="card col-view">
<a class="newsreleaseconsolidatelink display-outline w-100" href="/news-releases/oklo-awarded-450-million-contract-302902476.html">
<div class="col-sm-8 col-lg-9 pull-left card"> <h3> <small>11:19 ET</small> Oklo Awarded $450 Million Contract by U.S. Department of Defense </h3>
<p class="remove-outline">The contract covers the deployment of microreactors ...</p> </div> </a> </div> </div>
<div class="row newsCards"><div class="card col-view"><a class="newsreleaseconsolidatelink display-outline w-100" href="/news-releases/old-news-302800001.html">
<div class="card"><h3><small>Oct 07, 2026, 18:00 ET</small> Old news </h3><p class="remove-outline">x</p></div></a></div></div>"""


class PrnListTest(unittest.TestCase):
    def test_parse_cards(self) -> None:
        now = __import__("datetime").datetime(2026, 10, 8, 11, 20)
        items = bot.parse_prn_list(PRN_LIST_PAGE, now)
        self.assertEqual([i.title for i in items],
                         ["Oklo Awarded $450 Million Contract by U.S. Department of Defense", "Old news"])
        self.assertEqual(items[0].link, "https://www.prnewswire.com/news-releases/oklo-awarded-450-million-contract-302902476.html")
        et = __import__("datetime").datetime.fromtimestamp(items[0].published_ts, bot.eastern_tz())
        self.assertEqual((et.day, et.hour, et.minute), (8, 11, 19))
        self.assertEqual(bot.wire_item_ids(items[0])[1], "prn:302902476")

    def test_same_release_from_rss_is_not_handled_twice(self) -> None:
        page_item = bot.parse_prn_list(PRN_LIST_PAGE)[0]
        rss_item_ = bot.WireItem("some-guid", page_item.title, page_item.link + "?tc=eml", "", None, [])
        self.assertEqual(bot.wire_item_ids(page_item)[1], bot.wire_item_ids(rss_item_)[1])

    def test_ticker_from_the_release_page(self) -> None:
        async def scenario() -> None:
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            w = base_world()
            w.set(bot.PRN_LIST_URL, PRN_LIST_PAGE.replace("old-news", "older-news"))
            async with w.client() as client:
                radar = bot.Radar(make_cfg(Path(tmp.name), wire_feeds=[bot.PRN_LIST_URL]), client)
                await radar.refresh_tickers()
                await radar.poll_wire(bot.PRN_LIST_URL)                       # first poll: baseline
                w.set(bot.PRN_LIST_URL, PRN_LIST_PAGE.replace("302902476", "302902999"))
                w.set("https://www.prnewswire.com/news-releases/oklo-awarded-450-million-contract-302902999.html",
                      CONTRACT_PR)
                with mock.patch.object(bot, "SEC_MIN_INTERVAL", 0.0):
                    await radar.poll_wire(bot.PRN_LIST_URL)
                    await radar.drain(5)
            texts = [m["text"] for m in w.sent]
            self.assertEqual(len(texts), 1, texts)
            self.assertIn("<b>OKLO</b>", texts[0])

        run(scenario())
