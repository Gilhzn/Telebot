"""Check that the bot can push to Telegram: send one short, clearly marked test message to the chat
with what the bot pushed in the last day (from the state branch). Never prints the token or chat id.

    python tools/push_check.py state.json      # needs TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bot  # noqa: E402


def check_text(state: dict) -> str:
    log = state.get("alert_log") or []
    day = [a for a in log if a.get("t", 0) >= time.time() - 86400]
    last = max((a["t"] for a in log), default=None)
    last_s = dt.datetime.fromtimestamp(last, bot.eastern_tz()).strftime("%d.%m %H:%M") + " (שעון ניו יורק)" if last else "—"
    tickers = ", ".join(dict.fromkeys(a["ticker"] for a in day))[:300] or "—"
    sources = state.get("sources") or {}
    down = [k for k, v in sources.items() if isinstance(v, dict) and v.get("ok") is False]
    return (f"🧪 בדיקת פוש: הבוט מחובר לטלגרם ושולח.\n"
            f"התראות ב-24 השעות האחרונות: {len(day)} ({tickers})\n"
            f"התראה אחרונה: {last_s}\n"
            f"מקורות: {len(sources) - len(down)} תקינים" + (f", לא זמינים: {', '.join(down)}" if down else "") +
            "\nזו הודעת בדיקה, לא התראה.")


async def main() -> None:
    cfg = bot.Config.from_env()
    if not cfg.telegram_token or not cfg.chat_id:
        raise SystemExit("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing")
    state = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8")) if len(sys.argv) > 1 else {}
    async with httpx.AsyncClient(timeout=20) as client:
        me = (await client.get(f"https://api.telegram.org/bot{cfg.telegram_token}/getMe")).json()
        print("getMe ok:", me.get("ok"), "bot:", (me.get("result") or {}).get("username"))
        await bot.Telegram(client, cfg.telegram_token).send(cfg.chat_id, check_text(state))
    print("test message sent")


if __name__ == "__main__":
    if "--print" in sys.argv:
        print(check_text(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))))
    else:
        asyncio.run(main())
