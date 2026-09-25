"""Offline tests for bot.py — every HTTP call goes to an httpx.MockTransport.

Run: python -m unittest discover -s tests -v
"""
from __future__ import annotations

import asyncio
import json
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

    def test_bonuses_only_with_positive_score(self) -> None:
        self.assertEqual(bot.rule_score("Company mentions NVIDIA and $900 million").score, 0)
        res = bot.rule_score("Company announces strategic partnership with NVIDIA")
        self.assertEqual(res.score, 4)  # 2 + mega-company bonus 2
        self.assertEqual(bot.rule_score("NVIDIA announces strategic partnership", "NVIDIA Corp").score, 2)

    def test_amount_bonus(self) -> None:
        self.assertEqual(bot.rule_score("Receives purchase order worth $120M").score, 3)
        self.assertEqual(bot.rule_score("Receives purchase order worth $12M").score, 2)
        self.assertEqual(bot.rule_score("Receives purchase order worth $1.5 billion").score, 3)

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
