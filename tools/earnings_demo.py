"""Send one sample earnings alert to Telegram (a fictional company, clearly marked as a test),
built by the bot's own code path: Benzinga headlines -> earnings_signal -> earnings_score -> format_alert.

    python tools/earnings_demo.py          # needs TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID
"""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402

HEADLINES = [
    "Acme Robotics Q3 Adj. EPS $0.42 Beats $0.33 Estimate, Sales $1.284B Beat $1.221B Estimate",
    "Acme Robotics Raises FY2026 Adj EPS Guidance from $1.55-$1.65 to $1.80-$1.88 vs $1.62 Est",
]


def demo_text() -> str:
    score, reason = bot.earnings_score(bot.earnings_signal(HEADLINES))
    c = bot.Candidate(source="wire", source_label=bot.ALPACA_SOURCE, ticker="ACME", company="Acme Robotics Inc. (חברה לדוגמה)",
                      title=HEADLINES[0], link="https://www.benzinga.com/", published_ts=time.time() - 4, watch=False)
    return "🧪 התראת דוגמה: דוח רבעוני (חברה בדויה, המספרים לדוגמה)\n\n" + bot.format_alert(c, score, reason, mcap=4.6e9)


async def main() -> None:
    cfg = bot.Config.from_env()
    async with httpx.AsyncClient(timeout=20) as client:
        await bot.Telegram(client, cfg.telegram_token).send(cfg.chat_id, demo_text())
    print("sent")


if __name__ == "__main__":
    if "--print" in sys.argv:
        print(demo_text())
    else:
        asyncio.run(main())
