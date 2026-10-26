from datetime import date, datetime, timezone, timedelta
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "tests"))
from parquet_support import PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON, pa, pd, pq

AVAILABLE, SKIP_REASON = PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON

from hybrid_quant.data_registry import DataNotReady, DatasetRegistry
from hybrid_quant.market_data import StagedMarketData


@unittest.skipUnless(AVAILABLE, SKIP_REASON)
class MarketDataTests(unittest.TestCase):
    def setUp(self):
        temporary_root = PROJECT / ".test-tmp"
        temporary_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=temporary_root)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.price_path = self.root / "000001.parquet"
        self.price_rows = [dict(symbol="000001", trade_date=date(2024, 1, day), open=10.0, high=11.0,
                                low=9.0, close=10.5, volume_raw=10, volume_shares_estimate=1000)
                           for day in (2, 3)]
        self.write_price(self.price_rows)
        self.manifest_path = self.root / "prices.json"
        self.manifest = {"backtest_ready": False, "readiness_gaps": ["historical_status"],
                         "files": [{"symbol": "000001", "rows": 2, "start": "2024-01-02", "end": "2024-01-03",
                                    "output": str(self.price_path), "output_sha256": self.price_hash}]}
        self.save_manifest()
        self.registry_path = self.root / "datasets.json"
        self.registry_path.write_text(json.dumps({"datasets": {"stock_daily": {"manifest": "prices.json"}}}), encoding="utf-8")
        self.registry = DatasetRegistry(self.registry_path, self.root)
        self.adapter = StagedMarketData(self.registry)

    def write_price(self, rows):
        pq.write_table(pa.Table.from_pylist(rows), self.price_path)
        self.price_hash = hashlib.sha256(self.price_path.read_bytes()).hexdigest()

    def save_manifest(self):
        self.manifest_path.write_text(json.dumps(self.manifest), encoding="utf-8")

    def test_preview_preserves_raw_volume_and_unknown_time_status_and_adjustment(self):
        self.assertEqual(self.adapter.catalog("stock_daily", q="0000")["total"], 1)
        preview = self.adapter.preview_bars("stock_daily", "000001", limit=1)
        row = preview["rows"][0]
        self.assertFalse(preview["backtest_ready"])
        self.assertEqual(row["volume_raw"], 10)
        self.assertEqual(row["volume_shares_estimate"], 1000)
        for key in ("turnover", "adjusted_close", "available_at", "tradable"):
            self.assertIsNone(row[key])

    def test_hash_changed_output_is_refused_before_parquet_read(self):
        with self.price_path.open("ab") as stream:
            stream.write(b"changed")
        with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
            self.adapter.preview_bars("stock_daily", "000001")

    def test_duplicate_and_invalid_bars_are_not_repaired(self):
        rows = [self.price_rows[0], self.price_rows[0]]
        self.write_price(rows)
        self.manifest["files"][0]["output_sha256"] = self.price_hash
        self.save_manifest()
        with self.assertRaisesRegex(ValueError, "unique and ascending"):
            self.adapter.preview_bars("stock_daily", "000001")

    def test_request_paths_unknown_dataset_and_unbounded_limit_are_rejected(self):
        for dataset, symbol, limit in (("stock_daily", "../000001", 20), ("financials", "000001", 20), ("stock_daily", "000001", 501)):
            with self.subTest(dataset=dataset, symbol=symbol, limit=limit), self.assertRaises(ValueError):
                self.adapter.preview_bars(dataset, symbol, limit)
        with self.assertRaises(ValueError):
            self.adapter.catalog("stock_daily", q="x" * 33)

    def bundle(self):
        payload = {"schema_version": "1.0", "source_kind": "verified_history", "decision_at": "2024-01-03T16:30:00+08:00",
                   "securities": [{"symbol": "000001", "exchange": "SZSE", "asset_type": "stock",
                                   "listed_on": "2020-01-01", "delist_date": None, "buy_lot_size": 100,
                                   "settlement_sessions": 1, "industry_exposure": {"industry": 1.0}, "tracking_index": None,
                                   "historical_metadata_verified": True, "lookthrough_verified": True,
                                   "eligible_for_new_positions": True}],
                   "bars": [dict(symbol="000001", trade_date="2024-01-03", open=10, high=11, low=9, close=10.5,
                                 turnover=10000, available_at="2024-01-03T16:00:00+08:00", adjusted_close=10.5,
                                 volume=None, tradable=True, buyable=True, sellable=True)],
                   "factors": [{"symbol": "000001", "quality": 0.1, "value": 0.05, "available_at": "2024-01-04T10:00:00+08:00", "original_version_verified": False}],
                   "listing_age_sessions": {"000001": 300}}
        return payload

    def save_bundle(self, payload):
        content = json.dumps(payload).encode()
        (self.root / "snapshot.json").write_bytes(content)
        path = self.root / "bundle.json"
        path.write_text(json.dumps({"schema_version": "1.0", "source_kind": "verified_history",
                                    "readiness_gaps": [], "payload": "snapshot.json",
                                    "payload_sha256": hashlib.sha256(content).hexdigest()}), encoding="utf-8")
        return path

    def test_engine_boundary_rejects_real_staged_registry_before_bundle_read(self):
        with self.assertRaises(DataNotReady):
            self.adapter.load_snapshot(self.root / "does-not-exist.json")

    def test_verified_bundle_retains_future_unverified_factors_and_respects_cutoff(self):
        self.registry.validate_for_backtest = Mock(return_value=None)
        snapshot = self.adapter.load_snapshot(self.save_bundle(self.bundle()))
        self.registry.validate_for_backtest.assert_called_once()
        self.assertEqual(len(snapshot.known_history("000001")), 1)
        observation = snapshot.stock_factors["000001"][0]
        self.assertFalse(observation.original_version_verified)
        self.assertGreater(observation.available_at, snapshot.decision_at)

    def test_bundle_unknown_adjustments_and_forged_boolean_are_rejected(self):
        self.registry.validate_for_backtest = Mock(return_value=None)
        payload = self.bundle()
        payload["bars"][0]["adjusted_close"] = None
        with self.assertRaises(ValueError):
            self.adapter.load_snapshot(self.save_bundle(payload))
        payload = self.bundle()
        payload["securities"][0]["historical_metadata_verified"] = "false"
        with self.assertRaises(ValueError):
            self.adapter.load_snapshot(self.save_bundle(payload))

    def test_hand_written_ready_flag_cannot_admit_a_bundle(self):
        # A manifest flag is not evidence (design §5.2): the registry gate plus
        # an empty declared-gap list decide, and a flag alone changes nothing.
        self.registry.validate_for_backtest = Mock(side_effect=DataNotReady("criteria not met"))
        with self.assertRaises(DataNotReady):
            self.adapter.load_snapshot(self.save_bundle(self.bundle()))

    def test_pending_derivation_gaps_refuse_the_bundle(self):
        self.registry.validate_for_backtest = Mock(return_value=None)
        path = self.save_bundle(self.bundle())
        manifest = json.loads(path.read_text(encoding="utf-8"))
        manifest["readiness_gaps"] = ["bar_available_at_assumed_session_close"]
        path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaises(DataNotReady):
            self.adapter.load_snapshot(path)

    def test_security_master_and_volume_fields_must_be_explicit(self):
        self.registry.validate_for_backtest = Mock(return_value=None)
        payload = self.bundle()
        del payload["securities"][0]["exchange"]
        with self.assertRaises(ValueError):
            self.adapter.load_snapshot(self.save_bundle(payload))
        payload = self.bundle()
        payload["securities"][0]["exchange"] = "BSE"
        with self.assertRaises(ValueError):
            self.adapter.load_snapshot(self.save_bundle(payload))
        payload = self.bundle()
        payload["bars"][0]["volume"] = -5
        with self.assertRaises(ValueError):
            self.adapter.load_snapshot(self.save_bundle(payload))
