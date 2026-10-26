"""Wire-verification checks for the Eastmoney daily collector.

The collector's verify_reply is the discipline that keeps a provider's stale,
partial or misrouted reply out of the archive, so each rejection path is
checked against a synthetic payload here.
"""
from datetime import date
import importlib.util
import sys
import unittest
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))


def load_script(name: str):
    spec = importlib.util.spec_from_file_location(name.replace(".", "_"), PROJECT / "scripts" / name)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fetch = load_script("fetch_eastmoney_daily.py")


def kline(day: str, open_: float = 10.0, close: float = 10.5, high: float = 11.0,
          low: float = 9.5, volume: float = 1000.0, amount: float = 10500.0) -> str:
    return ",".join(str(value) for value in (day, open_, close, high, low, volume, amount,
                                             1.0, 0.5, 0.5, 0.2))


def payload(code: str, lines: list[str]) -> dict:
    return {"data": {"code": code, "klines": lines}}


ITEM = {"code": "600000", "start": "2018-01-01", "end": "2026-07-07"}


class VerifyReplyTests(unittest.TestCase):
    def test_a_well_formed_reply_normalizes_every_field(self):
        rows = fetch.verify_reply(payload("600000", [kline("2024-01-02"), kline("2024-01-03")]), ITEM)
        self.assertEqual([row["trade_date"] for row in rows], ["2024-01-02", "2024-01-03"])
        self.assertEqual(rows[0]["symbol"], "600000")
        self.assertEqual(rows[0]["volume_shares"], 1000.0)
        self.assertEqual(rows[0]["amount_cny"], 10500.0)

    def test_a_reply_for_another_security_is_refused(self):
        with self.assertRaises(ValueError) as caught:
            fetch.verify_reply(payload("600001", [kline("2024-01-02")]), ITEM)
        self.assertIn("not 600000", str(caught.exception))

    def test_rows_outside_the_requested_window_are_refused(self):
        for day in ("2017-12-29", "2026-07-08"):
            with self.subTest(day=day), self.assertRaises(ValueError):
                fetch.verify_reply(payload("600000", [kline(day)]), ITEM)

    def test_repeated_sessions_and_bad_bars_are_refused(self):
        with self.assertRaises(ValueError):
            fetch.verify_reply(payload("600000", [kline("2024-01-02"), kline("2024-01-02")]), ITEM)
        with self.assertRaises(ValueError):
            fetch.verify_reply(payload("600000", [kline("2024-01-02", high=9.0, low=9.5)]), ITEM)

    def test_a_reply_without_klines_is_refused(self):
        with self.assertRaises(ValueError):
            fetch.verify_reply({"data": {"code": "600000"}}, ITEM)
        with self.assertRaises(ValueError):
            fetch.verify_reply({"data": None}, ITEM)

    def test_secid_maps_the_two_markets(self):
        self.assertEqual(fetch.secid_of("600000"), "1.600000")
        self.assertEqual(fetch.secid_of("688001"), "1.688001")
        self.assertEqual(fetch.secid_of("000001"), "0.000001")
        self.assertEqual(fetch.secid_of("300750"), "0.300750")
        self.assertEqual(fetch.secid_of("159915"), "0.159915")


if __name__ == "__main__":
    unittest.main()
