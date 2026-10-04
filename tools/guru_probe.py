"""Dry run of the guru (13F) tracker on real SEC data: prints the Telegram message for each
tracked investor without sending anything."""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402


async def send_one(cik: int) -> None:
    """Send one real guru message to the configured Telegram chat, marked as a test."""
    cfg = bot.Config.from_env()
    cfg.state_file = Path("guru-probe-state.json")
    guru = next(g for g in cfg.gurus if g[0] == cik)
    async with bot.make_client() as client:
        radar = bot.Radar(cfg, client)
        await radar.refresh_tickers()
        real_reply = radar.reply

        async def as_test(text: str) -> None:
            await real_reply("🧪 <b>הודעת בדיקה</b> (מעקב משקיעי-על)\n\n" + text)
        radar.reply = as_test  # type: ignore[method-assign]
        await radar.check_guru(guru)
        print("sent test message for", guru[1])


async def main() -> None:
    cfg = bot.Config.from_env()
    cfg.state_file = Path("guru-probe-state.json")
    cfg.chat_id = "dry-run"
    cfg.guru_alerts = True
    async with bot.make_client() as client:
        radar = bot.Radar(cfg, client)
        await radar.refresh_tickers()
        sent: list[str] = []

        async def capture(text: str) -> None:
            sent.append(text)
        radar.reply = capture  # type: ignore[method-assign]
        for guru in cfg.gurus:
            resp = await radar.fetcher.get(bot.SEC_SUBMISSIONS_URL.format(cik=guru[0]))
            print(f"\n===== {guru[1]} (CIK {guru[0]}): SEC name = {resp.json().get('name')!r}")
            before = len(sent)
            try:
                await radar.check_guru(guru)
            except Exception as exc:  # noqa: BLE001
                print("   ERROR", bot.describe_error(exc))
            print(sent[-1] if len(sent) > before else "   (no 13F-HR found)")


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "--send":
        asyncio.run(send_one(int(sys.argv[2])))
    else:
        os.environ.setdefault("TELEGRAM_BOT_TOKEN", "0:dry-run")
        asyncio.run(main())
