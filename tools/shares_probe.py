"""Which SEC share-count facts exist for companies whose dei cover-page count is missing (companyconcept 404)?

    python tools/shares_probe.py CRWV,ODD,RSKD,ANGX,XNDU
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

ua = {"User-Agent": bot.Config.from_env().sec_user_agent or "StockNewsRadar research contact@example.com"}
tickers = bot.TickerMap()
tickers.load(httpx.get(bot.TICKERS_EXCHANGE_URL, headers=ua, timeout=30).json())
for sym in sys.argv[1].split(","):
    found = tickers.lookup(sym)
    if not found:
        print(sym, "not in the ticker map")
        continue
    r = httpx.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{found[0]:010d}.json", headers=ua, timeout=30)
    print(f"\n{sym} {found[1]} cik {found[0]}: companyfacts HTTP {r.status_code}, {len(r.content) // 1024} KB")
    if r.status_code != 200:
        continue
    for tax, concepts in (r.json().get("facts") or {}).items():
        for name, c in concepts.items():
            units = c.get("units") or {}
            if "shares" in units and "share" in name.lower():
                last = max(units["shares"], key=lambda u: u.get("end", ""))
                print(f"  {tax}:{name} latest end {last.get('end')} val {last.get('val'):,} form {last.get('form')} filed {last.get('filed')}")
    time.sleep(0.3)
