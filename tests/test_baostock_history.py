import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location("baostock_history_collector", Path(__file__).resolve().parents[1] / "scripts/fetch_baostock_history.py")
collector = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(collector)


class HistoryObservationTest(unittest.TestCase):
    def row(self, **kwargs):
        record = dict(zip(collector.FIELDS.split(","), ["2024-01-02", "sz.000001", "10", "11", "9", "10.5", "10", "12300", "129150", "3", "0.1", "1", "5", "0"]))
        record.update(kwargs)
        return [record[key] for key in collector.FIELDS.split(",")]

    def parse(self, rows):
        return collector.normalize_rows(collector.FIELDS.split(","), rows, "sz.000001", "2024-01-01", "2024-01-10")

    def test_reported_status_does_not_grant_execution_or_publication_time(self):
        item = self.parse([self.row()])[0]
        self.assertEqual(item["volume_shares_reported"], 12300)
        self.assertEqual(item["turnover_cny_reported"], 129150)
        self.assertEqual(item["provider_trade_status"], 1)
        for key in ("adjusted_close", "available_at", "tradable", "buyable", "sellable"):
            self.assertIsNone(item[key])

    def test_halt_zero_or_missing_prices_remain_explicit_observations(self):
        item = self.parse([self.row(tradestatus="0", open="", high="0", low="0", close="0", volume="0", amount="0", isST="")])[0]
        self.assertEqual(item["provider_trade_status"], 0)
        self.assertIsNone(item["open"])
        self.assertIsNone(item["provider_is_st"])

    def test_malformed_active_ohlc_and_status_are_rejected(self):
        for kwargs in (dict(high="8"), dict(close="0"), dict(volume="12.5"), dict(amount="nan"), dict(tradestatus="false"), dict(adjustflag="2"), dict(code="sz.000002")):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.parse([self.row(**kwargs)])

    def test_duplicate_dates_and_out_of_window_are_rejected(self):
        for rows in ([self.row(), self.row()], [self.row(date="2024-01-11")]):
            with self.assertRaises(ValueError):
                self.parse(rows)

    def test_complete_wire_send_records_only_sent_bytes_and_eof_is_failure(self):
        class Connection:
            def send(self, value):
                return 2
            def recv(self, count):
                return b""
        sock = collector.WireSocket(Connection())
        self.assertEqual(sock.send(b"abc"), 2)
        self.assertEqual(sock.sent, [b"ab"])
        with self.assertRaises(ConnectionError):
            sock.recv(10)


if __name__ == "__main__":
    unittest.main()
