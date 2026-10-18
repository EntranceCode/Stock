"""Factor-builder checks on a synthetic, self-contained evidence tree.

The fixtures mirror the registered products' real schemas, including the
provider's string report dates (``2026-06-30 00:00:00``) and the availability
layer's ISO strings. Every documented rule is covered: cumulative periods,
TTM roll-forward, same-currency components, non-positive earnings, missing
components, unknown announcement dates, and the unverified-version flag that
must never be asserted.
"""
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
sys.path.insert(0, str(PROJECT / "tests"))
from parquet_support import PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON, pa, pq

from hybrid_quant.factors import FactorError, FactorRequest, build_factors, load_factor_index

TZ = timezone(timedelta(hours=8))
# Four report periods per year across two years, cumulative by construction.
REPORT_DATES = [date(2023, 3, 31), date(2023, 6, 30), date(2023, 9, 30), date(2023, 12, 31),
                date(2024, 3, 31), date(2024, 6, 30), date(2024, 9, 30), date(2024, 12, 31)]
# Cumulative net profit that rises within each year, mirroring real filings.
CUMULATIVE_PROFIT = {date(2023, 3, 31): 1000.0, date(2023, 6, 30): 2200.0,
                     date(2023, 9, 30): 3600.0, date(2023, 12, 31): 5200.0,
                     date(2024, 3, 31): 1300.0, date(2024, 6, 30): 2800.0,
                     date(2024, 9, 30): 4500.0, date(2024, 12, 31): 6400.0}
# Announcement is roughly two months after the period end; availability states it.
ANNOUNCEMENTS = {day: day + timedelta(days=60) for day in REPORT_DATES}
PRICE_DAYS = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 6, 3), date(2024, 12, 31)]
PRICE_CLOSE = 10.0


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_table(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


@unittest.skipUnless(PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON)
class FactorBuilderTests(unittest.TestCase):
    def setUp(self):
        temporary_root = PROJECT / ".test-tmp"
        temporary_root.mkdir(exist_ok=True)
        self.root = Path(tempfile.mkdtemp(dir=temporary_root))
        self.addCleanup(self.remove_tree, self.root)
        self.write_fixtures()
        self.request = FactorRequest(availability_dir=self.root / "availability")

    @staticmethod
    def remove_tree(root: Path) -> None:
        if not root.exists():
            return
        for item in sorted(root.rglob("*"), key=lambda path: len(path.parts), reverse=True):
            if item.is_file():
                item.unlink()
            else:
                item.rmdir()

    def write_fixtures(self):
        # A stock with complete history, one with a missing FY2023 profit, one
        # whose announcement date is unknown, one with negative trailing profit,
        # one with negative equity, and one with no price rows at all.
        finance_files = []
        for symbol in ("600000", "600001", "600002", "600003", "600004", "600005"):
            rows = []
            for day in REPORT_DATES:
                profit = CUMULATIVE_PROFIT[day]
                if symbol == "600001" and day == date(2023, 12, 31):
                    profit = None
                if symbol == "600003":
                    profit = -abs(profit)
                rows.append({
                    "SECURITY_CODE": symbol,
                    # The provider stores report dates as strings with a time part.
                    "REPORT_DATE": f"{day.isoformat()} 00:00:00",
                    "CURRENCY": "CNY",
                    "NOTICE_DATE": f"{ANNOUNCEMENTS[day].isoformat()} 00:00:00",
                    "announcement_date_unknown": symbol == "600002",
                    "PARENTNETPROFIT": profit,
                    "TOTAL_SHARE": 1_000_000_000,
                    "BPS": -1.0 if symbol == "600004" else 5.0,
                })
            path = self.root / "financials" / f"{symbol}_financials.parquet"
            write_table(path, rows)
            finance_files.append({"symbol": symbol, "path": str(path), "rows": len(rows)})
        (self.root / "financials" / "manifest.json").write_text(
            json.dumps({"files": finance_files}), encoding="utf-8")

        # Daily prices: a flat series, absent for 600005.
        price_files = []
        for symbol in ("600000", "600001", "600002", "600003", "600004"):
            path = self.root / "prices" / f"{symbol}_daily.parquet"
            write_table(path, [{"trade_date": day, "close": PRICE_CLOSE} for day in PRICE_DAYS])
            price_files.append({"symbol": symbol, "path": str(path), "rows": len(PRICE_DAYS),
                                "sha256": sha256(path)})
        (self.root / "prices" / "manifest.json").write_text(
            json.dumps({"files": price_files}), encoding="utf-8")

        # Availability layer as ISO strings, mirroring the real product.
        availability_rows = []
        for symbol in ("600000", "600001", "600002", "600003", "600004", "600005"):
            for day in REPORT_DATES:
                if symbol == "600002":
                    continue
                availability_rows.append({"symbol": symbol,
                                          "report_date": day.isoformat(),
                                          "available_at": ANNOUNCEMENTS[day].isoformat()})
        path = self.root / "availability" / "availability.parquet"
        write_table(path, availability_rows)
        (self.root / "availability" / "manifest.json").write_text(
            json.dumps({"files": [{"path": str(path), "rows": len(availability_rows)}]}), encoding="utf-8")

        (self.root / "config").mkdir(exist_ok=True)
        (self.root / "config" / "datasets.json").write_text(json.dumps({"datasets": {
            "financials": {"manifest": "financials/manifest.json"},
            "stock_daily": {"manifest": "prices/manifest.json"},
        }}), encoding="utf-8")

    def build(self, **overrides) -> dict:
        fields = {"availability_dir": self.request.availability_dir}
        fields.update(overrides)
        return build_factors(self.root, FactorRequest(**fields), self.root / "out")

    def observations(self, index: dict, symbol: str) -> dict[str, dict]:
        return load_factor_index(self.root / "out" / index["run_id"]).get(symbol, {})

    def test_cumulative_periods_produce_ttm_roll_forward_scores(self):
        index = self.build(symbols=("600000",))
        rows = self.observations(index, "600000")
        self.assertEqual(len(rows), 8)
        # 2024-06-30: FY2023 (5200) + H1-2024 (2800) - H1-2023 (2200) = 5800.
        latest = rows["2024-06-30"]
        self.assertAlmostEqual(latest["ttm_net_profit"], 5800.0)
        # quality = 5800 / (5.0 x 1e9 shares) = 0.00116.
        self.assertAlmostEqual(latest["quality"], 5800.0 / 5e9)
        # value = 5800 / (10.0 x 1e9) = 0.00058, price is the last close on or
        # before the availability date (2024-08-29 -> the 2024-06-03 close).
        self.assertAlmostEqual(latest["value"], 5800.0 / 1e10)
        self.assertAlmostEqual(latest["price_basis"], PRICE_CLOSE)
        # Availability is the day after the announcement, at midnight +08:00.
        self.assertEqual(str(latest["available_at"]), "2024-08-30 00:00:00+08:00")
        # No observation may claim a verified original version.
        self.assertTrue(all(not row["original_version_verified"] for row in rows.values()))

    def test_missing_ttm_component_yields_no_score_with_a_reason(self):
        index = self.build(symbols=("600001",))
        rows = self.observations(index, "600001")
        blocked = rows["2024-06-30"]
        self.assertIsNone(blocked["quality"])
        self.assertIsNone(blocked["value"])
        # The FY2023 row exists but its profit is empty, which is reported
        # distinctly from a component row that is absent altogether.
        self.assertIn("ttm_component_profit_missing", list(blocked["reasons"]))
        # The annual report itself still works: its TTM is the annual figure.
        annual = rows["2024-12-31"]
        self.assertAlmostEqual(annual["ttm_net_profit"], 6400.0)

    def test_unknown_announcement_date_excludes_the_observation(self):
        index = self.build(symbols=("600002",))
        self.assertEqual(index["symbols"], 1)
        rows = self.observations(index, "600002")
        for row in rows.values():
            self.assertIsNone(row["available_at"])
            self.assertIsNone(row["quality"])
            self.assertIn("announcement_date_unknown", list(row["reasons"]))

    def test_nonpositive_trailing_earnings_yield_no_value_score(self):
        index = self.build(symbols=("600003",))
        rows = self.observations(index, "600003")
        latest = rows["2024-06-30"]
        self.assertIsNone(latest["value"])
        self.assertIn("nonpositive_ttm_earnings", list(latest["reasons"]))
        # Quality may stay negative; only value requires positive earnings.
        self.assertLess(latest["quality"], 0)

    def test_nonpositive_equity_yields_no_quality_score(self):
        index = self.build(symbols=("600004",))
        rows = self.observations(index, "600004")
        latest = rows["2024-06-30"]
        self.assertIsNone(latest["quality"])
        self.assertIn("nonpositive_equity", list(latest["reasons"]))

    def test_missing_prices_yield_no_value_but_keep_quality(self):
        index = self.build(symbols=("600005",))
        rows = self.observations(index, "600005")
        latest = rows["2024-06-30"]
        self.assertIsNone(latest["value"])
        self.assertIn("price_or_shares_unavailable", list(latest["reasons"]))
        self.assertIsNotNone(latest["quality"])

    def test_mixed_currency_ttm_component_is_refused(self):
        # Rebuild one symbol with a foreign-currency prior-year component.
        path = self.root / "financials" / "600006_financials.parquet"
        rows = []
        for day in REPORT_DATES:
            rows.append({"SECURITY_CODE": "600006", "REPORT_DATE": f"{day.isoformat()} 00:00:00",
                         "CURRENCY": "CNY" if day.year == 2024 else "USD",
                         "NOTICE_DATE": f"{ANNOUNCEMENTS[day].isoformat()} 00:00:00",
                         "announcement_date_unknown": False,
                         "PARENTNETPROFIT": CUMULATIVE_PROFIT[day],
                         "TOTAL_SHARE": 1_000_000_000, "BPS": 5.0})
        write_table(path, rows)
        manifest = json.loads((self.root / "financials" / "manifest.json").read_text(encoding="utf-8"))
        manifest["files"].append({"symbol": "600006", "path": str(path), "rows": len(rows)})
        (self.root / "financials" / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        price_path = self.root / "prices" / "600006_daily.parquet"
        write_table(price_path, [{"trade_date": day, "close": PRICE_CLOSE} for day in PRICE_DAYS])
        # The availability layer must also cover the added symbol.
        availability_path = self.root / "availability" / "availability.parquet"
        availability = json.loads((self.root / "availability" / "manifest.json").read_text(encoding="utf-8"))
        rows = [{"symbol": "600006", "report_date": day.isoformat(),
                 "available_at": ANNOUNCEMENTS[day].isoformat()} for day in REPORT_DATES]
        write_table(availability_path, rows)
        availability["files"] = [{"path": str(availability_path), "rows": len(rows)}]
        (self.root / "availability" / "manifest.json").write_text(json.dumps(availability), encoding="utf-8")
        index = self.build(symbols=("600006",))
        rows = self.observations(index, "600006")
        latest = rows["2024-06-30"]
        self.assertIsNone(latest["ttm_net_profit"])
        self.assertIn("ttm_component_currency_mismatch", list(latest["reasons"]))

    def test_registered_hash_mismatch_skips_the_symbol(self):
        manifest_path = self.root / "financials" / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["files"][0]["sha256"] = "0" * 64
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        index = self.build(symbols=("600000",))
        self.assertEqual(index["symbols"], 0)
        self.assertIn("registered_hash_mismatch", index["exclusion_reasons"])

    def test_unregistered_output_hash_is_recorded_not_refused(self):
        index = self.build(symbols=("600000",))
        entry = index["files"][0]
        self.assertTrue(entry["financials_sha256"])
        self.assertFalse(entry["financials_sha256_registered"])
        self.assertEqual(index["financials_outputs_without_registered_hash"], 1)

    def test_requested_symbols_not_found_are_reported(self):
        index = self.build(symbols=("600000", "999999"))
        self.assertEqual(index["requested_symbols_not_found"], ["999999"])

    def test_product_records_formulas_and_readiness_gaps(self):
        index = self.build(symbols=("600000",))
        self.assertEqual(index["formulas"]["quality"],
                         "ttm_net_profit / (BPS x TOTAL_SHARE at the report date)")
        self.assertIn("original_announcement_vintages_unverified", index["readiness_gaps"])
        self.assertIn("financial_sector_fields_not_separately_reviewed", index["readiness_gaps"])
        self.assertFalse(index["synthetic"])

    def test_missing_availability_layer_is_refused(self):
        with self.assertRaises(FactorError):
            self.build(availability_dir=self.root / "absent")


if __name__ == "__main__":
    unittest.main()
