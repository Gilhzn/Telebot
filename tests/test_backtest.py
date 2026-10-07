"""Offline tests for tools/backtest.py (trade simulation, signal selection, report)."""
from __future__ import annotations

import datetime as dt
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import backtest  # noqa: E402

ET = backtest.ET


def et(y: int, m: int, d: int, hh: int, mm: int, ss: int = 0) -> float:
    return dt.datetime(y, m, d, hh, mm, ss, tzinfo=ET).timestamp()


def minute_bars(start: float, prices: list[float]) -> list[backtest.Bar]:
    """One bar per minute; open = previous close, high/low = max/min of open and close."""
    bars, prev = [], prices[0]
    for i, p in enumerate(prices):
        bars.append((start + 60 * i, prev, max(prev, p), min(prev, p), p, 10_000))
        prev = p
    return bars


class SimulateTest(unittest.TestCase):
    def test_entry_is_first_bar_at_or_after_three_minutes(self) -> None:
        start = et(2026, 3, 10, 10, 0)
        bars = minute_bars(start, [10.0] * 5 + [11.0] * 200)
        sig = et(2026, 3, 10, 10, 1, 30)             # 10:01:30 -> target 10:04:30
        sim = backtest.simulate(bars, sig)
        assert sim is not None
        self.assertEqual(sim["entry_ts"], et(2026, 3, 10, 10, 5))   # first bar starting >= 10:04:30
        self.assertGreaterEqual(sim["entry_delay_s"], 180)
        self.assertLess(sim["entry_delay_s"], 240)
        self.assertEqual(sim["entry"], 10.0)           # open of the 10:05 bar = close of 10:04
        self.assertAlmostEqual(sim["pre_move"], 0.0)
        self.assertAlmostEqual(sim["r_5m"], 0.10)
        self.assertTrue(sim["tradable"])
        self.assertEqual(sim["session"], "regular")

    def test_exact_minute_signal_enters_at_exactly_three_minutes(self) -> None:
        start = et(2026, 3, 10, 10, 0)
        bars = minute_bars(start, [10.0 + 0.01 * i for i in range(300)])
        sim = backtest.simulate(bars, et(2026, 3, 10, 10, 2))
        assert sim is not None
        self.assertEqual(sim["entry_delay_s"], 180)

    def test_horizon_returns_use_the_close_h_minutes_after_entry(self) -> None:
        start = et(2026, 3, 10, 10, 0)
        prices = [10.0] * 3 + [10.0 + 0.1 * i for i in range(1, 200)]
        bars = minute_bars(start, prices)
        sim = backtest.simulate(bars, et(2026, 3, 10, 10, 0))   # entry bar 10:03, open 10.0
        assert sim is not None
        # after 2 minutes: close of the 10:04 bar = 10.2
        self.assertAlmostEqual(sim["r_2m"], 0.02, places=6)

    def test_take_profit_and_conservative_stop(self) -> None:
        entry_ts = et(2026, 3, 10, 10, 0)
        day = [(entry_ts, 10, 10, 10, 10, 1), (entry_ts + 60, 10, 10.5, 9.0, 10, 1)]
        # the second bar touches both +2% and -2%: the stop counts
        self.assertEqual(backtest.tp_sl(day, 10.0, entry_ts, 0.02, 0.02), -0.02)
        day = [(entry_ts, 10, 10, 10, 10, 1), (entry_ts + 60, 10, 10.5, 9.9, 10, 1)]
        self.assertEqual(backtest.tp_sl(day, 10.0, entry_ts, 0.02, 0.02), 0.02)

    def test_overnight_signal_is_not_tradable_at_three_minutes(self) -> None:
        bars = minute_bars(et(2026, 3, 11, 4, 0), [5.0] * 60)
        sim = backtest.simulate(bars, et(2026, 3, 10, 22, 0))
        assert sim is not None
        self.assertFalse(sim["tradable"])
        self.assertEqual(sim["session"], "closed")

    def test_sessions(self) -> None:
        self.assertEqual(backtest.session_of(et(2026, 3, 10, 8, 0)), "pre")
        self.assertEqual(backtest.session_of(et(2026, 3, 10, 9, 30)), "regular")
        self.assertEqual(backtest.session_of(et(2026, 3, 10, 16, 5)), "after")
        self.assertEqual(backtest.session_of(et(2026, 3, 14, 12, 0)), "closed")   # Saturday


class ParseTest(unittest.TestCase):
    def test_accepted_time_and_ticker(self) -> None:
        page = '<div class="infoHead">Accepted</div>\n<div class="info">2026-03-10 16:05:12</div>'
        self.assertEqual(backtest.parse_accepted(page), dt.datetime(2026, 3, 10, 16, 5, 12, tzinfo=ET))
        self.assertEqual(backtest.point_in_time_ticker(
            ["Barnes & Noble Education, Inc. (BNED) (CIK 0001634117)"]), "BNED")
        self.assertEqual(backtest.point_in_time_ticker(
            ["Foo Corp (FOO, FOO-WT) (CIK 0000000001)"]), "FOO")
        self.assertIsNone(backtest.point_in_time_ticker(["Private Co (CIK 0000000002)"]))


class PipelineTest(unittest.TestCase):
    def test_selection_dedup_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            with mock.patch.multiple(backtest, OUT=out, SIGNALS=out / "signals", TRADES=out / "trades.jsonl"):
                backtest.SIGNALS.mkdir()
                base = et(2026, 3, 10, 10, 0)
                rows = [
                    {"adsh": "a", "ticker": "AAA", "ts": base, "score": 5, "rejected": None},
                    {"adsh": "b", "ticker": "AAA", "ts": base + 3600, "score": 5, "rejected": None},  # dup
                    {"adsh": "c", "ticker": "BBB", "ts": base, "score": 3, "rejected": None},        # low
                    {"adsh": "d", "ticker": "CCC", "ts": base, "score": 4, "rejected": "הנפקה"},     # rejected
                    {"adsh": "e", "ticker": "DDD", "ts": base + 60, "score": 4, "rejected": None},
                ]
                (backtest.SIGNALS / "2026-03-10.jsonl").write_text(
                    "".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
                picked = backtest.load_signals()
                self.assertEqual([r["adsh"] for r in picked], ["a", "e"])

                bars = minute_bars(base - 600, [10.0] * 10 + [10.0 + 0.05 * i for i in range(400)])
                trades = []
                for r in picked:
                    sim = backtest.simulate(bars, r["ts"])
                    trades.append({**r, **sim, "form": "8-K", "pump": "none", "accepted": "2026-03-10T10:00:00"})
                backtest.TRADES.write_text("".join(json.dumps(t) + "\n" for t in trades), encoding="utf-8")
                summary = backtest.report()
                self.assertEqual(summary["tradable"], 2)
                text = (out / "report.md").read_text(encoding="utf-8")
                self.assertIn("30 דקות", text)
                self.assertIn("$2,000", text)


if __name__ == "__main__":
    unittest.main()


class GainersHistoryTest(unittest.TestCase):
    def test_gainer_days_from_spark(self) -> None:
        import gainers
        t = lambda d: dt.datetime(2026, 10, d, 9, 30, tzinfo=backtest.ET).timestamp()  # noqa: E731
        data = {"SXTC": {"timestamp": [t(5), t(6), t(7)], "close": [1.2, 1.25, 2.83]},
                "AAPL": {"timestamp": [t(5), t(6), t(7)], "close": [100, 101, 102]},
                "CHEAP": {"timestamp": [t(6), t(7)], "close": [0.1, 0.2]}}
        days = gainers.gainer_days(data, dt.date(2026, 10, 1))
        self.assertEqual([(d["ticker"], d["date"]) for d in days], [("SXTC", "2026-10-07")])
        self.assertAlmostEqual(days[0]["pct"], 126.4)
