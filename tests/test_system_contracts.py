from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest

from hybrid_quant.data_registry import DataNotReady, DatasetRegistry, REQUIRED_DATASETS
from hybrid_quant.models import AssetType, DailyBar, DataSnapshot, Order, Security, Side, StockFactorObservation, StrategyTarget

TZ = timezone(timedelta(hours=8))


class SystemContractTests(unittest.TestCase):
    def test_snapshot_excludes_not_yet_available_and_future_date_bars(self):
        cutoff = datetime(2024, 1, 2, 16, 30, tzinfo=TZ)
        known = DailyBar("X", date(2024, 1, 2), 10, 11, 9, 10, 100, cutoff)
        late = DailyBar("X", date(2024, 1, 3), 10, 11, 9, 10, 100, cutoff + timedelta(days=1))
        snapshot = DataSnapshot(cutoff, {}, {"X": (known, late)})
        self.assertEqual(snapshot.known_history("X"), (known,))

    def test_invalid_dates_prices_and_short_weights_are_rejected(self):
        with self.assertRaises(ValueError):
            StockFactorObservation("X", 1, 1, datetime(2024, 1, 2))
        with self.assertRaises(ValueError):
            DailyBar("X", date(2024, 1, 2), 10, 9, 8, 10, 100, datetime(2024, 1, 2, 16, tzinfo=TZ))
        with self.assertRaises(ValueError):
            StrategyTarget("stock", datetime(2024, 1, 2, 16, tzinfo=TZ), {"X": -0.1})

    def test_registry_blocks_staged_data_and_missing_required_roles(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "staged.json").write_text(json.dumps({"backtest_ready": False, "readiness_gaps": ["historical_status"]}), encoding="utf-8")
            path = root / "datasets.json"
            path.write_text(json.dumps({"datasets": {"stock_daily": {"manifest": "staged.json"}}}), encoding="utf-8")
            registry = DatasetRegistry(path, root)
            self.assertFalse(registry.statuses()[0].manifest_declares_ready)
            with self.assertRaises(DataNotReady) as caught:
                registry.validate_for_backtest()
            self.assertIn("historical evidence incomplete", str(caught.exception))
            self.assertIn("missing datasets", str(caught.exception))

    def test_registry_cannot_escape_project_with_external_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "datasets.json"
            path.write_text(json.dumps({"datasets": {"stock_daily": {"manifest": "../outside.json"}}}), encoding="utf-8")
            with self.assertRaises(ValueError):
                DatasetRegistry(path, root).statuses()

    def test_flag_without_file_evidence_cannot_admit_a_dataset(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pretend.json").write_text(json.dumps({"backtest_ready": True, "readiness_gaps": [], "files": []}), encoding="utf-8")
            path = root / "datasets.json"
            path.write_text(json.dumps({"datasets": {key: {"manifest": "pretend.json"} for key in REQUIRED_DATASETS}}), encoding="utf-8")
            with self.assertRaises(DataNotReady):
                DatasetRegistry(path, root).validate_for_backtest()

    def test_quantity_and_settlement_cannot_be_fractional_or_boolean(self):
        for value in (100.5, True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                Order("o", "X", Side.BUY, value, datetime(2024, 1, 2, 16, tzinfo=TZ), date(2024, 1, 3), 10)
        for rules in ({"buy_lot_size": 100.5}, {"settlement_sessions": 0.5}, {"settlement_sessions": True}):
            with self.subTest(rules=rules), self.assertRaises(ValueError):
                Security("X", AssetType.STOCK, date(2020, 1, 1), **rules)

    def test_string_false_cannot_become_verified_financial_data(self):
        with self.assertRaises(ValueError):
            StockFactorObservation("X", 1, 1, datetime(2024, 1, 2, 16, tzinfo=TZ), "false")

    def test_source_response_hash_does_not_verify_a_normalized_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pretend.json").write_text(json.dumps({"backtest_ready": True, "files": [{"path": "x.parquet", "source_sha256": "source-only"}]}), encoding="utf-8")
            path = root / "datasets.json"
            path.write_text(json.dumps({"datasets": {key: {"manifest": "pretend.json"} for key in REQUIRED_DATASETS}}), encoding="utf-8")
            with self.assertRaises(DataNotReady) as caught:
                DatasetRegistry(path, root).validate_for_backtest()
            self.assertIn("output hash", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
