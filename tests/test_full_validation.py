"""Full-validation harness checks on a synthetic series directory.

The harness reads a written series, so the fixture writes the same files the
builder produces. Assertions cover the report structure, the accounting red
lines, the slippage ordering, and the gate behaviour: with the gates as the
evidence leaves them the engine must hold cash, and every relaxed run must
reconcile.
"""
from datetime import date, datetime, time, timedelta, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
sys.path.insert(0, str(PROJECT / "tests"))
from parquet_support import PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON

import importlib.util

spec = importlib.util.spec_from_file_location("full_validation", PROJECT / "scripts/run_full_validation.py")
full_validation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(full_validation)

TZ = timezone(timedelta(hours=8))
SESSION_COUNT = 130
SESSIONS = [date(2024, 1, 2) + timedelta(days=offset) for offset in range(SESSION_COUNT)]
SYMBOLS = ("600000", "600004", "600006")


def write_series(root: Path) -> Path:
    """Write the same files the builder produces, with trailing windows."""
    series_dir = root / "series"
    (series_dir / "sessions").mkdir(parents=True)
    sessions = []
    for index, day in enumerate(SESSIONS):
        directory = series_dir / "sessions" / day.isoformat()
        directory.mkdir()
        securities = [{"symbol": symbol, "exchange": "SSE", "asset_type": "stock",
                       "listed_on": "1999-11-10", "delist_date": None, "buy_lot_size": 100,
                       "settlement_sessions": 1, "industry_exposure": {},
                       "tracking_index": None, "historical_metadata_verified": False,
                       "lookthrough_verified": False, "eligible_for_new_positions": False}
                      for symbol in SYMBOLS]
        window = SESSIONS[max(0, index - 129):index + 1]
        bars = []
        for symbol in SYMBOLS:
            for position, bar_day in enumerate(window):
                price = 10.0 + 0.01 * position + 0.02 * index
                bars.append({"symbol": symbol, "trade_date": bar_day.isoformat(),
                             "open": price, "high": price, "low": price, "close": price,
                             "turnover": 1e8,
                             "available_at": datetime.combine(bar_day, time(16, 30), TZ).isoformat(),
                             "adjusted_close": price, "volume": None,
                             "tradable": True, "buyable": True, "sellable": True})
        factors = []
        for symbol in SYMBOLS:
            if index >= 2:
                factors.append({"symbol": symbol, "quality": 0.08 + 0.001 * SYMBOLS.index(symbol),
                                "value": 0.05,
                                "available_at": datetime.combine(SESSIONS[1], time(0, 0), TZ).isoformat(),
                                "original_version_verified": False})
        payload = {"decision_at": datetime.combine(day, time(16, 30), TZ).isoformat(),
                   "securities": securities, "bars": bars, "factors": factors,
                   "listing_age_sessions": {symbol: 5000 for symbol in SYMBOLS}}
        (directory / "payload.json").write_text(json.dumps(payload), encoding="utf-8")
        (directory / "manifest.json").write_text(json.dumps(
            {"trade_date": day.isoformat(), "payload_sha256": "0" * 64}), encoding="utf-8")
        sessions.append({"trade_date": day.isoformat(),
                         "manifest": f"sessions/{day.isoformat()}/manifest.json"})
    (series_dir / "series.json").write_text(json.dumps({
        "series_id": "test-series", "sessions": sessions,
        "symbols": {"included": list(SYMBOLS)},
        "self_check": {"engine_compatible": True}}), encoding="utf-8")
    return series_dir


def remove_tree(root: Path) -> None:
    if not root.exists():
        return
    for item in sorted(root.rglob("*"), key=lambda path: len(path.parts), reverse=True):
        if item.is_file():
            item.unlink()
        else:
            item.rmdir()


@unittest.skipUnless(PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON)
class FullValidationTests(unittest.TestCase):
    """One gated and one relaxed harness run per class, shared by all checks."""

    @classmethod
    def setUpClass(cls):
        temporary_root = PROJECT / ".test-tmp"
        temporary_root.mkdir(exist_ok=True)
        cls.root = Path(tempfile.mkdtemp(dir=temporary_root))
        cls.addClassCleanup(remove_tree, cls.root)
        cls.series = write_series(cls.root)
        cls.gated = cls.run_harness(relaxed=False)
        cls.relaxed = cls.run_harness(relaxed=True)

    @classmethod
    def run_harness(cls, relaxed):
        output = cls.root / ("relaxed" if relaxed else "gated")
        code = full_validation.main(["--series", str(cls.series), "--out", str(output),
                                     *(["--relax-gates"] if relaxed else [])])
        assert code == 0
        return json.loads((output / "test-series.json").read_text(encoding="utf-8"))

    def test_gates_closed_produces_no_orders_and_records_the_mode(self):
        self.assertEqual(self.gated["gates"], "as_evidence_leaves_them")
        self.assertEqual(self.gated["relaxation_scope"], [])
        for name, run in self.gated["ablations"].items():
            self.assertEqual(run["orders"], 0, name)
            self.assertEqual(run["fills"], 0, name)

    def test_relaxed_runs_reconcile_and_cover_every_section(self):
        report = self.relaxed
        self.assertEqual(report["gates"], "relaxed_for_mechanics_check")
        self.assertEqual(len(report["relaxation_scope"]), 3)
        self.assertEqual(set(report["ablations"]),
                         {"hybrid_full", "stock_only", "etf_only", "defensive_only", "no_stock"})
        self.assertEqual(set(report["slippage_sensitivity"]), {"0bps", "5bps", "10bps", "20bps"})
        self.assertIn("sealed_oos_20", report["time_split"])
        self.assertIn("benchmark", report)
        for name, run in report["ablations"].items():
            self.assertTrue(run["holdings_match"], name)
            self.assertEqual(run["cash_difference"], 0, name)
            self.assertEqual(run["negative_cash_sessions"], 0, name)
        for step, run in report["slippage_sensitivity"].items():
            self.assertTrue(run["holdings_match"], step)
            self.assertEqual(run["negative_cash_sessions"], 0, step)

    def test_slippage_monotonically_reduces_final_nav(self):
        report = self.relaxed
        navs = [report["slippage_sensitivity"][step]["final_nav"]
                for step in ("0bps", "5bps", "10bps", "20bps")]
        self.assertEqual(navs, sorted(navs, reverse=True))
        # Trade counts must not change: slippage prices the same orders.
        counts = {report["slippage_sensitivity"][step]["fills"]
                  for step in ("0bps", "5bps", "10bps", "20bps")}
        self.assertEqual(len(counts), 1)

    def test_benchmark_holds_the_same_universe(self):
        benchmark = self.relaxed["benchmark"]
        self.assertEqual(benchmark["symbols_held"], len(SYMBOLS))
        self.assertGreater(benchmark["sessions"], 1)


if __name__ == "__main__":
    unittest.main()
