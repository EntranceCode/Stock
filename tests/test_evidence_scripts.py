"""Evidence-builder script checks on synthetic project trees.

The evidence scripts read registered datasets and write new registered runs
that the readiness criteria then evaluate. Each test builds a small synthetic
project with the exact registered shapes these scripts consume, and covers the
honest directions: a clean run produces the expected counts, a planted
disagreement is recorded rather than smoothed over, and registration refuses
runs whose inventory does not verify.
"""
from datetime import date, datetime, timedelta
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
sys.path.insert(0, str(PROJECT / "tests"))
from parquet_support import PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON, pa, pq


def load_script(name: str):
    spec = importlib.util.spec_from_file_location(name.replace(".", "_"), PROJECT / "scripts" / name)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


register = load_script("register_evidence_dataset.py")
residuals = load_script("build_adjustment_residual_evidence.py")
supplement = load_script("build_listing_supplement_evidence.py")
boundaries = load_script("build_delisting_boundary_evidence.py")


def write_parquet(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def write_json(path: Path, value: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    return path


class SyntheticProject:
    """A tiny registry with the shapes the evidence builders consume."""

    def __init__(self, root: Path):
        self.root = root
        (root / "config").mkdir(parents=True)
        self.datasets: dict[str, dict] = {}
        self._write_config()

    def _write_config(self) -> Path:
        return write_json(self.root / "config/datasets.json", {"datasets": self.datasets})

    def register(self, name: str, relative: str) -> None:
        self.datasets[name] = {"manifest": relative}
        self._write_config()


@unittest.skipUnless(PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON)
class RegisterEvidenceTests(unittest.TestCase):
    def setUp(self):
        temporary_root = PROJECT / ".test-tmp"
        temporary_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=temporary_root)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.project = SyntheticProject(self.root)

    def build_run(self, name: str, rows: list[dict], *, wrong_hash: str | None = None,
                  no_inventory: bool = False) -> Path:
        run = self.root / name
        path = write_parquet(run / "data.parquet", rows)
        digest = register.sha256(path)
        files = [] if no_inventory else [
            {"path": str(path), "sha256": wrong_hash or digest, "rows": len(rows)}]
        return write_json(run / "manifest.json", {"run_id": name, "files": files})

    def test_a_verified_run_is_registered_and_a_disagreeing_one_refuses(self):
        manifest = self.build_run("good", [{"a": 1}, {"a": 2}])
        config = register.register_dataset(self.root, "good", manifest)
        recorded = json.loads(config.read_text(encoding="utf-8"))["datasets"]["good"]
        self.assertEqual(Path(recorded["manifest"]), Path("good") / "manifest.json")
        # Already registered: left untouched.
        before = config.read_text(encoding="utf-8")
        register.register_dataset(self.root, "good", manifest)
        self.assertEqual(config.read_text(encoding="utf-8"), before)
        # Hash disagreement: refused, nothing written.
        tampered = self.build_run("bad", [{"a": 1}], wrong_hash="0" * 64)
        with self.assertRaises(ValueError) as caught:
            register.register_dataset(self.root, "bad", tampered)
        self.assertIn("hash mismatch", str(caught.exception))
        self.assertNotIn("bad", json.loads(config.read_text(encoding="utf-8"))["datasets"])
        # A manifest without a file inventory cannot be verified.
        empty = self.build_run("empty", [{"a": 1}], no_inventory=True)
        with self.assertRaises(ValueError) as caught:
            register.register_dataset(self.root, "empty", empty)
        self.assertIn("no file inventory", str(caught.exception))

    def test_run_row_counts_are_verified(self):
        manifest = self.build_run("rows", [{"a": 1}, {"a": 2}])
        value = json.loads(manifest.read_text(encoding="utf-8"))
        value["files"][0]["rows"] = 7
        manifest.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaises(ValueError) as caught:
            register.register_dataset(self.root, "rows", manifest)
        self.assertIn("row count mismatch", str(caught.exception))


@unittest.skipUnless(PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON)
class ResidualEvidenceTests(unittest.TestCase):
    """Factor-exactness audit: the factor ratio on an ex-date must equal the
    ratio implied by that day's plan against its own previous close."""

    def setUp(self):
        temporary_root = PROJECT / ".test-tmp"
        temporary_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=temporary_root)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.project = SyntheticProject(self.root)
        self.write_sources()

    def write_sources(self, *, wrong_factor: bool = False, ex_close: float = 9.7,
                      ex_high: float = 9.7) -> None:
        sessions = [date(2024, 1, 2) + timedelta(days=offset) for offset in (0, 1, 2, 3, 6)]
        closes = [10.0, 10.1, ex_close, ex_close + 0.05, ex_close + 0.10]
        # 000001: a cash dividend of 0.5 on the third session; the exact factor
        # ratio on the ex-date is 10.1 / (10.1 - 0.5) = 1.052083...
        exact = 10.1 / (10.1 - 0.5)
        factors = [1.0, 1.0, 1.03 if wrong_factor else exact, exact, exact]
        adjusted_rows = [{"trade_date": day, "adjustment_factor": factor,
                          "close_raw": close, "close_adjusted": close * factor}
                         for day, factor, close in zip(sessions, factors, closes)]
        path = write_parquet(self.root / "adjusted/000001_adjusted.parquet", adjusted_rows)
        adjusted_files = [{"symbol": "000001", "path": str(path),
                           "sha256": residuals.sha256(path), "rows": len(adjusted_rows)}]
        adjusted_manifest = write_json(self.root / "adjusted/manifest.json",
                                       {"run_id": "adj", "files": adjusted_files})
        self.project.register("adjusted_prices", "adjusted/manifest.json")

        raw_rows = [{"trade_date": day, "open": close, "high": max(close, ex_high) if i == 2 else close,
                     "low": close, "close": close}
                    for i, (day, close) in enumerate(zip(sessions, closes))]
        raw_path = write_parquet(self.root / "stock/000001_daily.parquet", raw_rows)
        stock_manifest = write_json(self.root / "stock/manifest.json", {
            "run_id": "stock",
            "files": [{"symbol": "000001", "path": str(raw_path), "rows": len(raw_rows),
                       "sha256": residuals.sha256(raw_path), "start": "2024-01-02",
                       "end": "2024-01-08"}]})
        self.project.register("stock_daily", "stock/manifest.json")

        events = [{"symbol": "000001", "ex_date": sessions[2], "event_type": "distribution",
                   "cash_per_old_unit_gross": 0.5, "shares_multiplier": 1.0,
                   "source_file": None}]
        events_path = write_parquet(self.root / "actions/events.parquet", events)
        events_manifest = write_json(self.root / "actions/manifest.json", {
            "run_id": "acts", "rows": len(events),
            "files": [{"path": str(events_path), "sha256": residuals.sha256(events_path),
                       "rows": len(events)}]})
        self.project.register("action_samples", "actions/manifest.json")
        self.sources = (adjusted_manifest, events_manifest, stock_manifest)

    def audit(self) -> tuple[dict, Path]:
        return residuals.build(self.root)

    def test_an_exact_series_audits_clean(self):
        manifest, manifest_path = self.audit()
        self.assertEqual(manifest["bars_audited"], 5)
        self.assertEqual(manifest["ex_dates_audited"], 1)
        self.assertEqual(manifest["ex_dates_beyond_price_limit_after_adjustment"], 0)
        self.assertEqual(manifest["ex_dates_within_limit_band"], 1)
        self.assertEqual(manifest["limit_band_observations"], 0)
        self.assertEqual((manifest_path.parent / "residual_jumps.jsonl").read_text(encoding="utf-8"), "")
        self.assertEqual(manifest["files"][0]["rows"], 0)

    def test_a_factor_that_misses_the_ex_reference_is_a_residual(self):
        self.write_sources(wrong_factor=True)
        manifest, manifest_path = self.audit()
        self.assertEqual(manifest["ex_dates_beyond_price_limit_after_adjustment"], 1)
        record = json.loads((manifest_path.parent / "residual_jumps.jsonl")
                            .read_text(encoding="utf-8").splitlines()[0])
        self.assertEqual(record["symbol"], "000001")
        self.assertAlmostEqual(record["applied_factor_ratio"], 1.03, places=6)
        self.assertAlmostEqual(record["event_implied_ratio"], 1.052083, places=6)
        self.assertEqual(record["cash_per_share"], 0.5)

    def test_a_raw_price_outside_the_limit_band_is_enumerated_separately(self):
        # The factor stays exact, but the raw close sits above the limit-up
        # around the ex-reference: a price observation, not a residual.
        self.write_sources(ex_close=10.70, ex_high=10.71)
        manifest, manifest_path = self.audit()
        self.assertEqual(manifest["ex_dates_beyond_price_limit_after_adjustment"], 0)
        self.assertEqual(manifest["limit_band_observations"], 1)
        record = json.loads((manifest_path.parent / "limit_band_observations.jsonl")
                            .read_text(encoding="utf-8").splitlines()[0])
        self.assertEqual(record["ex_reference_rounded"], 9.6)
        self.assertEqual(record["limit_up"], 10.56)
        self.assertEqual({item["field"] for item in record["outside"]}, {"close", "high"})

    def test_the_audit_is_registerable(self):
        _, manifest_path = self.audit()
        register.register_dataset(self.root, "adjustment_residuals", manifest_path, replace=True)
        recorded = json.loads((self.root / "config/datasets.json").read_text(encoding="utf-8"))
        self.assertIn("adjustment_residuals", recorded["datasets"])


@unittest.skipUnless(PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON)
class ListingSupplementTests(unittest.TestCase):
    def setUp(self):
        temporary_root = PROJECT / ".test-tmp"
        temporary_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=temporary_root)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.project = SyntheticProject(self.root)
        # Local universe of three codes; the official candidates cover two.
        entries = []
        for symbol in ("600000", "600079", "000001"):
            directory = self.root / "stock" / symbol
            path = write_parquet(directory / f"{symbol}.parquet", [{"close": 1.0}])
            entries.append({"symbol": symbol, "path": str(path), "rows": 1,
                            "start": "2024-01-02", "end": "2024-01-03"})
        stock_manifest = write_json(self.root / "stock/manifest.json",
                                    {"run_id": "stock", "files": entries})
        self.project.register("stock_daily", "stock/manifest.json")
        candidates = write_parquet(self.root / "meta/listing_candidates.parquet",
                                   [{"symbol": symbol, "exchange": "SSE",
                                     "source_listing_date": date(1999, 11, 10)}
                                    for symbol in ("600000", "000001")])
        meta_manifest = write_json(self.root / "meta/manifest.json", {
            "run_id": "meta", "coverage": {"local_codes_without_listing_reference": 1},
            "files": [{"dataset": "listing_candidates", "path": str(candidates),
                       "sha256": supplement.sha256(candidates), "rows": 2}]})
        self.project.register("security_metadata", "meta/manifest.json")
        provider_rows = [["sh.600000", "A", "1999-11-10", "", "1", "1"],
                         ["sh.600079", "B", "1997-06-06", "", "1", "1"],
                         ["sz.000001", "C", "1991-04-03", "", "1", "1"],
                         ["sh.600555", "D", "1999-01-18", "2008-04-14", "1", "0"]]
        provider_path = self.root / "universe/query_stock_basic.json"
        provider_path.parent.mkdir(parents=True, exist_ok=True)
        provider_path.write_text(json.dumps({"fields": ["code"], "rows": provider_rows}),
                                 encoding="utf-8")
        universe_manifest = write_json(self.root / "universe/manifest.json", {
            "run_id": "universe",
            "files": [{"path": str(provider_path),
                       "sha256": supplement.sha256(provider_path),
                       "rows": len(provider_rows)}]})
        self.project.register("etf_universe", "universe/manifest.json")
        self.sources = (stock_manifest, meta_manifest, universe_manifest)

    def test_the_uncovered_code_is_supplemented_with_provider_provenance(self):
        manifest, manifest_path = supplement.build(self.root)
        self.assertEqual(manifest["provider_references"], 1)
        self.assertEqual(manifest["still_without_reference"], [])
        record = json.loads((manifest_path.parent / "listing_supplement.jsonl")
                            .read_text(encoding="utf-8").splitlines()[0])
        self.assertEqual(record["symbol"], "600079")
        self.assertEqual(record["provider_listing_date"], "1997-06-06")
        self.assertFalse(record["official_reference"])
        self.assertTrue(Path(record["source_file"]).is_file())
        register.register_dataset(self.root, "listing_supplement", manifest_path)
        recorded = json.loads((self.root / "config/datasets.json").read_text(encoding="utf-8"))
        self.assertIn("listing_supplement", recorded["datasets"])


@unittest.skipUnless(PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON)
class DelistingBoundaryTests(unittest.TestCase):
    def setUp(self):
        temporary_root = PROJECT / ".test-tmp"
        temporary_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=temporary_root)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.project = SyntheticProject(self.root)
        # A local series that ends exactly on a termination date, plus a code
        # whose termination falls inside the data window with no local series.
        entries = []
        for symbol, end in (("600000", "2020-05-12"),):
            path = write_parquet(self.root / "stock" / f"{symbol}.parquet",
                                 [{"trade_date": date(2020, 5, 11), "close": 1.0},
                                  {"trade_date": date(2020, 5, 12), "close": 1.0}])
            entries.append({"symbol": symbol, "path": str(path), "rows": 2,
                            "start": "2020-05-11", "end": end})
        stock_manifest = write_json(self.root / "stock/manifest.json",
                                    {"run_id": "stock", "files": entries})
        self.project.register("stock_daily", "stock/manifest.json")
        candidates = write_parquet(self.root / "meta/listing_candidates.parquet", [
            {"symbol": "600000", "exchange": "SSE", "source_listing_date": date(1999, 11, 10),
             "source_termination_date": date(2020, 5, 12),
             "termination_boundary_verified": False, "source_file": "official.xlsx"},
            {"symbol": "000002", "exchange": "SZSE", "source_listing_date": date(1991, 4, 3),
             "source_termination_date": date(2019, 6, 20),
             "termination_boundary_verified": False, "source_file": "official.xlsx"},
        ])
        meta_manifest = write_json(self.root / "meta/manifest.json", {
            "run_id": "meta",
            "files": [{"dataset": "listing_candidates", "path": str(candidates),
                       "sha256": boundaries.sha256(candidates), "rows": 2}]})
        self.project.register("security_metadata", "meta/manifest.json")
        provider_rows = [["sh.600000", "A", "1999-11-10", "2020-05-12", "1", "0"],
                         ["sz.000002", "B", "1991-04-03", "2019-06-20", "1", "0"]]
        provider_path = self.root / "universe/query_stock_basic.json"
        provider_path.parent.mkdir(parents=True, exist_ok=True)
        provider_path.write_text(json.dumps({"fields": ["code"], "rows": provider_rows}),
                                 encoding="utf-8")
        universe_manifest = write_json(self.root / "universe/manifest.json", {
            "run_id": "universe",
            "files": [{"path": str(provider_path),
                       "sha256": boundaries.sha256(provider_path),
                       "rows": len(provider_rows)}]})
        self.project.register("etf_universe", "universe/manifest.json")
        self.sources = (stock_manifest, meta_manifest, universe_manifest)

    def test_each_terminated_security_records_its_decision_state(self):
        manifest, manifest_path = boundaries.build(self.root)
        self.assertEqual(manifest["terminated_securities"], 2)
        self.assertEqual(manifest["boundary_decidable"], 1)
        self.assertEqual(manifest["decision_state_counts"],
                         {"last_bar_equals_termination": 1, "no_price_series": 1})
        records = [json.loads(line) for line in
                   (manifest_path.parent / "delisting_boundaries.jsonl")
                   .read_text(encoding="utf-8").splitlines()]
        by_symbol = {record["symbol"]: record for record in records}
        self.assertTrue(by_symbol["600000"]["boundary_decidable"])
        self.assertEqual(by_symbol["600000"]["decision_state"], "last_bar_equals_termination")
        self.assertFalse(by_symbol["000002"]["boundary_decidable"])
        self.assertEqual(by_symbol["000002"]["decision_state"], "no_price_series")
        # The provider date corroborates where it exists.
        self.assertEqual(by_symbol["000002"]["provider_termination_date"], "2019-06-20")
        self.assertTrue(by_symbol["000002"]["provider_agrees_with_official"])
        register.register_dataset(self.root, "delisting_boundaries", manifest_path)
        recorded = json.loads((self.root / "config/datasets.json").read_text(encoding="utf-8"))
        self.assertIn("delisting_boundaries", recorded["datasets"])

    def test_an_observed_last_bar_before_termination_decides_the_boundary(self):
        # 600000's series now ends a week before its registered termination
        # date: the market's own record of the last tradable day.
        rows = [{"trade_date": date(2020, 5, 5), "close": 1.0},
                {"trade_date": date(2020, 5, 6), "close": 1.0}]
        path = write_parquet(self.root / "stock/600000_daily.parquet", rows)
        manifest = json.loads((self.root / "stock/manifest.json").read_text(encoding="utf-8"))
        manifest["files"][0].update(path=str(path), rows=2, start="2020-05-05", end="2020-05-06")
        (self.root / "stock/manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        result, _ = boundaries.build(self.root)
        self.assertEqual(result["decision_state_counts"],
                         {"last_bar_precedes_termination": 1, "no_price_series": 1})
        run = sorted((self.root / "data/external/delisting_boundaries").iterdir())[-1]
        record = [json.loads(line) for line in
                  (run / "delisting_boundaries.jsonl").read_text(encoding="utf-8").splitlines()]
        by_symbol = {item["symbol"]: item for item in record}
        self.assertTrue(by_symbol["600000"]["boundary_decidable"])
        self.assertEqual(by_symbol["600000"]["observed_last_bar_date"], "2020-05-06")
        self.assertEqual(by_symbol["600000"]["official_termination_date"], "2020-05-12")


if __name__ == "__main__":
    unittest.main()
