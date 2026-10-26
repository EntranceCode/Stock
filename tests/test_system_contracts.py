from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest

from hybrid_quant.data_registry import DataNotReady, DatasetRegistry, REQUIRED_DATASETS
from hybrid_quant.models import (AssetType, Constituent, CorporateAction, DailyBar, DataSnapshot,
                                 Fundamental, Order, Security, Side, StockFactorObservation, StrategyTarget)

TZ = timezone(timedelta(hours=8))


class SystemContractTests(unittest.TestCase):
    def test_snapshot_excludes_not_yet_available_and_future_date_bars(self):
        cutoff = datetime(2024, 1, 2, 16, 30, tzinfo=TZ)
        known = DailyBar("X", date(2024, 1, 2), 10, 11, 9, 10, 100, cutoff, 10.0)
        late = DailyBar("X", date(2024, 1, 3), 10, 11, 9, 10, 100, cutoff + timedelta(days=1), 10.0)
        snapshot = DataSnapshot(cutoff, {}, {"X": (known, late)})
        self.assertEqual(snapshot.known_history("X"), (known,))

    def test_known_history_is_repeatable_and_excludes_late_publications(self):
        cutoff = datetime(2024, 1, 5, 16, 30, tzinfo=TZ)
        bars = tuple(DailyBar("X", date(2024, 1, 2) + timedelta(days=index), 10, 11, 9, 10, 100,
                              cutoff + timedelta(days=index - 2), 10.0) for index in range(6))
        snapshot = DataSnapshot(cutoff, {}, {"X": bars})
        expected = tuple(bar for bar in bars
                         if bar.available_at <= cutoff and bar.trade_date <= cutoff.date())
        self.assertTrue(expected)
        self.assertLess(len(expected), len(bars))
        self.assertEqual(snapshot.known_history("X"), expected)
        self.assertEqual(snapshot.known_history("X"), snapshot.known_history("X"))
        self.assertEqual(snapshot.known_history("absent"), ())
        self.assertEqual(snapshot.known_history("absent"), ())

    def test_invalid_dates_prices_and_short_weights_are_rejected(self):
        with self.assertRaises(ValueError):
            StockFactorObservation("X", 1, 1, datetime(2024, 1, 2))
        with self.assertRaises(ValueError):
            DailyBar("X", date(2024, 1, 2), 10, 9, 8, 10, 100, datetime(2024, 1, 2, 16, tzinfo=TZ), 10.0)
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
            self.assertIn("missing datasets", str(caught.exception))
            # The declared flag is informational only; it can neither admit nor
            # refuse: the computed criteria decide, and they fail here.
            report = registry.readiness()
            self.assertFalse(report.ready)
            self.assertFalse(report.dataset("stock_daily").ready)

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

    def test_registry_validation_verifies_once_within_its_ttl(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "staged.json").write_text(json.dumps({"backtest_ready": False, "readiness_gaps": ["historical_status"]}), encoding="utf-8")
            path = root / "datasets.json"
            path.write_text(json.dumps({"datasets": {"stock_daily": {"manifest": "staged.json"}}}), encoding="utf-8")

            def counted(registry):
                verifications = []
                original = registry._verify_admission
                def verification():
                    verifications.append(1)
                    original()
                registry._verify_admission = verification
                return verifications

            cached = DatasetRegistry(path, root)
            calls = counted(cached)
            for _ in range(3):
                with self.assertRaises(DataNotReady):
                    cached.validate_for_backtest()
            self.assertEqual(len(calls), 1)
            fresh = DatasetRegistry(path, root, validation_ttl=0)
            calls = counted(fresh)
            for _ in range(3):
                with self.assertRaises(DataNotReady):
                    fresh.validate_for_backtest()
            self.assertEqual(len(calls), 3)

    def test_quantity_and_settlement_cannot_be_fractional_or_boolean(self):
        for value in (100.5, True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                Order("o", "X", Side.BUY, value, datetime(2024, 1, 2, 16, tzinfo=TZ), date(2024, 1, 3), 10)
        for rules in ({"buy_lot_size": 100.5}, {"settlement_sessions": 0.5}, {"settlement_sessions": True}):
            with self.subTest(rules=rules), self.assertRaises(ValueError):
                Security("X", AssetType.STOCK, date(2020, 1, 1), "SSE", **rules)

    def test_contract_required_fields_must_be_explicitly_supplied(self):
        """Design §3.3: a required field that is missing fails construction."""
        full_bar = dict(symbol="X", trade_date=date(2024, 1, 2), open=10.0, high=11.0,
                        low=9.0, close=10.5, turnover=100.0,
                        available_at=datetime(2024, 1, 2, 16, tzinfo=TZ), adjusted_close=10.5)
        for name in ("symbol", "trade_date", "open", "high", "low", "close", "turnover",
                     "available_at", "adjusted_close"):
            with self.subTest(object="DailyBar", field=name):
                missing = {key: value for key, value in full_bar.items() if key != name}
                with self.assertRaises(TypeError):
                    DailyBar(**missing)
        full_security = dict(symbol="X", asset_type=AssetType.STOCK, listed_on=date(2020, 1, 1),
                             exchange="SSE")
        for name in ("symbol", "asset_type", "listed_on", "exchange"):
            with self.subTest(object="Security", field=name):
                missing = {key: value for key, value in full_security.items() if key != name}
                with self.assertRaises(TypeError):
                    Security(**missing)
        # Unknown stays distinguishable from zero: volume is the D3-pending
        # field and an explicit null is accepted, a negative or fractional one is not.
        with self.assertRaises(ValueError):
            DailyBar(**full_bar, volume=-1)
        with self.assertRaises(ValueError):
            DailyBar(**full_bar, volume=1.5)
        self.assertIsNone(DailyBar(**full_bar).volume)
        # A termination record before listing is a contradiction, not data.
        with self.assertRaises(ValueError):
            Security("X", AssetType.STOCK, date(2020, 1, 1), "SSE", delist_date=date(2019, 1, 1))
        # The exchange venue is explicit; a venue outside the v1 scope is refused.
        with self.assertRaises(ValueError):
            Security("X", AssetType.STOCK, date(2020, 1, 1), "BSE")

    def test_core_object_contracts_validate_their_degraded_fields(self):
        verified = datetime(2024, 4, 21, tzinfo=TZ)
        fundamental = Fundamental("X", date(2023, 12, 31), date(2024, 4, 20), verified, 1.5, "provider_latest")
        self.assertFalse(fundamental.first_disclosure_verified)
        with self.assertRaises(ValueError):  # available before announced
            Fundamental("X", date(2023, 12, 31), date(2024, 4, 20), datetime(2024, 4, 19, tzinfo=TZ), 1.5, "v")
        with self.assertRaises(ValueError):  # an empty caliber version hides the D2 decision
            Fundamental("X", date(2023, 12, 31), date(2024, 4, 20), verified, 1.5, "")
        action = CorporateAction("X", "cash_dividend", date(2024, 6, 20),
                                 cash_per_share=0.8144, cash_basis="common_account")
        self.assertIsNone(action.pay_date)
        with self.assertRaises(ValueError):  # cash without its caliber is a silent merge
            CorporateAction("X", "cash_dividend", date(2024, 6, 20), cash_per_share=0.8)
        with self.assertRaises(ValueError):  # a pay date before the ex date is impossible
            CorporateAction("X", "cash_dividend", date(2024, 6, 20),
                            cash_per_share=0.8, cash_basis="common_account", pay_date=date(2024, 6, 19))
        constituent = Constituent("510300", "沪深300", date(2024, 6, 28), verified)
        self.assertIsNone(constituent.weights)  # supply state C stays explicitly unprovided
        with self.assertRaises(ValueError):
            Constituent("510300", "沪深300", date(2024, 6, 28), verified, weights={"X": 1.2})

    def test_snapshot_observations_cannot_be_rewritten_by_their_caller(self):
        """Design §3.3: equal inputs construct equal snapshots that outsiders cannot alter."""
        cutoff = datetime(2024, 1, 5, 16, 30, tzinfo=TZ)
        securities = {"X": Security("X", AssetType.STOCK, date(2020, 1, 1), "SSE",
                                    historical_metadata_verified=True, eligible_for_new_positions=True)}
        histories = {"X": (DailyBar("X", date(2024, 1, 2), 10, 11, 9, 10, 100, cutoff, 10.0),)}
        first = DataSnapshot(cutoff, securities, histories)
        second = DataSnapshot(cutoff, dict(securities), {key: list(value) for key, value in histories.items()})
        self.assertEqual(first, second)
        securities.pop("X")
        histories["X"] = ()
        self.assertEqual(first.known_history("X"), second.known_history("X"))
        self.assertEqual(len(first.known_history("X")), 1)

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
