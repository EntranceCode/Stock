import csv
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from hybrid_quant.dashboard_store import DashboardStore

PROJECT = Path(__file__).resolve().parents[1]
RUN_ID = "20261009T105850Z_48901e2a"


class DashboardStoreTests(unittest.TestCase):
    def setUp(self):
        temporary_root = PROJECT / ".test-tmp"
        temporary_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=temporary_root)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.run_dir = self.root / "reports/demo" / RUN_ID
        shutil.copytree(PROJECT / "reports/demo" / RUN_ID, self.run_dir)
        self.manifest_path = self.run_dir / "manifest.json"
        self.manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        for entry in self.manifest["files"]:
            entry["path"] = str(self.run_dir / Path(entry["path"]).name)
        self.save_manifest()
        self.store = DashboardStore(self.root)

    def save_manifest(self):
        self.manifest_path.write_text(json.dumps(self.manifest), encoding="utf-8")

    def refresh_hash(self, name):
        for entry in self.manifest["files"]:
            if Path(entry["path"]).name == name:
                entry["sha256"] = hashlib.sha256((self.run_dir / name).read_bytes()).hexdigest()
        self.save_manifest()

    def test_read_only_view_parses_real_saved_demo_and_paginates(self):
        result = self.store.run_detail(RUN_ID)
        self.assertEqual(len(result["equity"]), 80)
        self.assertIsInstance(result["equity"][-1]["holdings"], dict)
        self.assertEqual(result["final_holdings"], result["equity"][-1]["holdings"])
        self.assertTrue(result["validation_only"])
        self.assertFalse(result["investment_performance"])
        records = self.store.run_records(RUN_ID, "fills", offset=10, limit=12)
        self.assertEqual(len(records["items"]), 12)
        self.assertEqual(records["total"], 197)

    def test_changed_output_is_invalid_and_other_runs_remain_listed(self):
        with (self.run_dir / "fills.jsonl").open("a", encoding="utf-8") as stream:
            stream.write("{}\n")
        item = self.store.runs()["items"][0]
        self.assertFalse(item["valid"])
        self.assertIn("hash mismatch", item["error"])

    def test_synthetic_labels_cannot_be_removed_or_reclassified(self):
        self.manifest["investment_performance"] = True
        self.save_manifest()
        with self.assertRaises(ValueError):
            self.store.run_detail(RUN_ID)

    def test_path_cannot_escape_run_even_if_file_is_inside_project(self):
        outside = self.root / "outside.json"
        outside.write_text("{}", encoding="utf-8")
        self.manifest["files"][0]["path"] = str(outside)
        self.manifest["files"][0]["sha256"] = hashlib.sha256(outside.read_bytes()).hexdigest()
        self.save_manifest()
        with self.assertRaises(ValueError):
            self.store.run_detail(RUN_ID)
        for name in ("../outside", "bad", "20269999T999999Z_12345678"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.store.run_detail(name)

    def test_nonfinite_account_value_is_rejected_even_with_new_file_hash(self):
        path = self.run_dir / "equity.csv"
        with path.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            fields, rows = reader.fieldnames, list(reader)
        rows[0]["nav"] = "nan"
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        self.refresh_hash("equity.csv")
        with self.assertRaises(ValueError):
            self.store.run_detail(RUN_ID)

    def test_missing_registry_and_reports_are_unknown_instead_of_zero_ready(self):
        overview = self.store.overview()
        self.assertFalse(overview["historical_data_admitted"])
        self.assertEqual(overview["admission"]["status"], "unavailable")
        self.assertIsNone(overview["retired_prices"]["missing_codes"])
        self.assertTrue(overview["warnings"])
        self.assertIsNotNone(overview["latest_run"])

    def test_pagination_and_record_file_are_constrained(self):
        for kwargs in ({"kind": "../config"}, {"kind": "fills", "offset": -1}, {"kind": "fills", "limit": 201}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.store.run_records(RUN_ID, **kwargs)
