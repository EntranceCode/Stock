"""Checks prevent destructive timestamp fixes and premature backtest admission."""
from pathlib import Path
from datetime import date
import json
import sys
import tempfile
import unittest

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "scripts"))
sys.path.insert(0, str(PROJECT / ".deps" / "parquet"))
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from verify_local_source import decode_daily_dates
from normalize_local_daily import normalize_frame, require_backtest_ready
from normalize_action_samples import cash_per_unit, split_multiplier, gross_return, stock_event_values, apply_issuer_evidence
from fetch_stock_actions import validate_page


class NormalizationTests(unittest.TestCase):
    def sample(self):
        return pd.DataFrame({"time": [1783350, 1783436], "open": [10., 11.],
                             "high": [11., 12.], "low": [9., 10.], "close": [10.5, 11.5],
                             "tick_volume": [100, 200]})

    def test_reference_dates_preserve_every_row(self):
        source = self.sample()
        output = normalize_frame(source, "000001", "2026-10-07")
        self.assertEqual([str(d) for d in output.trade_date], ["2026-07-06", "2026-07-07"])
        self.assertEqual(output.source_time.tolist(), source.time.tolist())
        self.assertEqual(output.volume_raw.tolist(), [100, 200])
        self.assertEqual(output.volume_shares_estimate.tolist(), [10000, 20000])

    def test_duplicate_buckets_fail_instead_of_dropping_bars(self):
        with self.assertRaises(ValueError):
            decode_daily_dates(pd.Series([1783436, 1783436]))

    def test_malformed_and_future_dates_fail(self):
        for times in ([1783435], [1783436, 1783350], [238310953]):
            with self.subTest(times=times), self.assertRaises(ValueError):
                decode_daily_dates(pd.Series(times))
        with self.assertRaises(ValueError):
            decode_daily_dates(pd.Series([1783436]), as_of="2026-07-06")

    def test_bad_prices_and_volume_fail(self):
        for field, value in [("high", 1), ("close", -1), ("tick_volume", -1)]:
            frame = self.sample()
            frame.loc[0, field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                normalize_frame(frame, "000001", "2026-10-07")

    def test_index_and_etf_codes_require_separate_adapters(self):
        for code in ["idx_000300", "510300", "159915"]:
            with self.subTest(code=code), self.assertRaises(ValueError):
                normalize_frame(self.sample(), code, "2026-10-07")

    def test_missing_or_unverified_metadata_cannot_enter_backtest(self):
        with tempfile.TemporaryDirectory(dir=PROJECT / "tests") as directory:
            self.assertTrue(Path(directory).resolve().is_relative_to((PROJECT / "tests").resolve()))
            path = Path(directory) / "daily.parquet"
            table = pa.Table.from_pandas(self.sample(), preserve_index=False)
            pq.write_table(table, path)
            with self.assertRaises(ValueError):
                require_backtest_ready(path)
            table = table.replace_schema_metadata({b"stock.provenance": b'{"backtest_ready": false, "readiness_gaps": ["corporate_actions"]}'})
            pq.write_table(table, path)
            with self.assertRaises(ValueError):
                require_backtest_ready(path)

    def test_dividend_and_split_preserve_theoretical_wealth(self):
        self.assertAlmostEqual(gross_return(10, 9.9, cash=cash_per_unit("每10份派现金1.0000元")), 0)
        self.assertAlmostEqual(gross_return(10, 5, shares_multiplier=split_multiplier("1:2")), 0)
        self.assertAlmostEqual(gross_return(10, 4.9, shares_multiplier=2, cash=0.2), 0)

    def test_unknown_event_units_are_rejected(self):
        for text in ["每份派现金1元", "暂无分红信息!", "1:0"]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                (split_multiplier if text.startswith("1:") else cash_per_unit)(text)

    def test_nonfinite_event_values_are_rejected(self):
        for name in ("previous_close", "close", "shares_multiplier", "cash"):
            for value in (float("nan"), float("inf"), float("-inf")):
                inputs = {"previous_close": 10, "close": 10, "shares_multiplier": 1, "cash": 0}
                inputs[name] = value
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    gross_return(**inputs)

    def test_stock_units_must_match_implemented_description(self):
        row = {"IMPL_PLAN_PROFILE": "10送2股转增3股派1.20元(含税)", "PRETAX_BONUS_RMB": 1.2, "BONUS_IT_RATIO": 5}
        self.assertEqual(stock_event_values(row), (0.12, 1.5))
        for altered in ({**row, "PRETAX_BONUS_RMB": 12}, {**row, "BONUS_IT_RATIO": float("nan")},
                        {**row, "IMPL_PLAN_PROFILE": "每股派1.2元"}):
            with self.assertRaises(ValueError):
                stock_event_values(altered)

    def test_incomplete_or_changed_pagination_is_rejected(self):
        rows = [{"SECURITY_CODE": "000001", "EX_DIVIDEND_DATE": "2025-01-01", "REPORT_DATE": "2024-12-31"}]
        payload = {"success": True, "result": {"count": 2, "pages": 2, "data": rows}}
        self.assertEqual(validate_page(payload, 1, 1)[1], (2, 2))
        with self.assertRaises(ValueError):
            validate_page(payload, 1, 1, expected=(3, 3))
        with self.assertRaises(ValueError):
            validate_page({"success": True, "result": {"count": 2, "pages": 1, "data": rows}}, 1, 2)

    def test_issuer_units_and_end_of_day_split_do_not_invent_ex_session(self):
        with tempfile.TemporaryDirectory(dir=PROJECT / "tests") as directory:
            source = Path(directory)
            evidence = {"fund_code": "511010", "cash_unit": "CNY_per_fund_unit", "source_url": "https://example.invalid",
                        "records": [["2025-09-23", "2025-09-22", "2025-09-26", "1.4500"]],
                        "table_html": "<table><thead><tr><th>除息日</th><th>权益登记日</th><th>红利分配日</th><th>每份分红(元)</th></tr></thead><tbody><tr><td>2025-09-23</td><td>2025-09-22</td><td>2025-09-26</td><td>1.4500</td></tr></tbody></table>"}
            (source / "511010-dividends.json").write_text(json.dumps(evidence), encoding="utf-8")
            proof = {"symbol": "510500", "event_type": "share_split", "effective_date": "2022-08-26",
                     "announcement_date": "2022-08-23", "shares_multiplier": "1.14539", "effective_phase": "end_of_day",
                     "rounding_rule": "ceil_each_holder_result_to_integer_units", "price_adjustment_date_candidate": "2022-08-29", "source_url": "https://example.invalid"}
            (source / "manifest.json").write_text(json.dumps({"evidence": [proof]}), encoding="utf-8")
            events = [{"symbol": "510500", "event_type": "split", "source_event_date": date(2022, 8, 26), "ex_date": None}]
            apply_issuer_evidence(events, source, date(2018, 1, 1), date(2026, 10, 9))
            self.assertEqual(events[1]["cash_per_old_unit_gross"], 1.45)
            self.assertEqual(events[1]["payment_date"], date(2025, 9, 26))
            self.assertEqual(events[0]["shares_multiplier"], 1.14539)
            self.assertIsNone(events[0]["ex_date"])
            evidence["cash_unit"] = "CNY_per_ten_units"
            (source / "511010-dividends.json").write_text(json.dumps(evidence), encoding="utf-8")
            with self.assertRaises(ValueError):
                apply_issuer_evidence([], source, date(2018, 1, 1), date(2026, 10, 9))


if __name__ == "__main__":
    unittest.main()
