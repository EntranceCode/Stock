"""Registration-script checks on synthetic project trees.

The two scripts under ``scripts/`` rewrite registered manifests: the finalizer
adds output hashes, and the availability builder registers a new evidence
dataset. Both must verify before they write and must refuse to overwrite
disagreeing evidence, so the tests cover the happy path, the tamper paths and
idempotence against a synthetic project root.
"""
from datetime import date
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


finalize_script = load_script("finalize_dataset_registration.py")
availability_script = load_script("build_financial_availability_evidence.py")


def write_parquet(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


class SyntheticProject:
    """A minimal project tree holding a financials run and its raw captures."""

    def __init__(self, root: Path, *, corrupted_notice: str | None = None):
        self.root = root
        self.entries = []
        for symbol in ("000001", "000002"):
            directory = root / "data/external/financials/run1"
            notice = f"2026-04-2{0 if symbol.endswith('1') else 1} 00:00:00"
            if symbol == "000002" and corrupted_notice is not None:
                notice = corrupted_notice
            rows = [{"SECURITY_CODE": symbol, "REPORT_DATE": "2026-03-31 00:00:00",
                     "NOTICE_DATE": notice}]
            write_parquet(directory / f"{symbol}_financials.parquet", rows)
            raw_dir = root / "data/external/financials/raw/run1/raw"
            raw_dir.mkdir(parents=True, exist_ok=True)
            capture = {"payload": {"result": {"data": [
                {"REPORT_DATE": "2026-03-31 00:00:00", "NOTICE_DATE": "2026-04-20 00:00:00"}]}}}
            (raw_dir / f"{symbol}.json").write_text(json.dumps(capture), encoding="utf-8")
            self.entries.append({
                "symbol": symbol, "rows": 1, "path": str(directory / f"{symbol}_financials.parquet"),
                "unknown_announcement_rows": 0,
                "source_original_file": str(raw_dir / f"{symbol}.json"),
                "source_sha256": availability_script.sha256(raw_dir / f"{symbol}.json"),
            })
        manifest = {"run_id": "run1", "provider": "synthetic", "cached_snapshot": str(raw_dir.parent),
                    "files": self.entries, "rows": 2}
        self.manifest_path = root / "data/external/financials/run1/manifest.json"
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self.manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        (root / "config").mkdir(parents=True, exist_ok=True)
        (root / "config/datasets.json").write_text(json.dumps(
            {"datasets": {"financials": {"manifest": "data/external/financials/run1/manifest.json"}}}),
            encoding="utf-8")

    def manifest(self) -> dict:
        return json.loads(self.manifest_path.read_text(encoding="utf-8"))


@unittest.skipUnless(PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON)
class FinalizeRegistrationTests(unittest.TestCase):
    def setUp(self):
        temporary_root = PROJECT / ".test-tmp"
        temporary_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=temporary_root)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.project = SyntheticProject(self.root)

    def test_financials_output_hashes_are_registered_and_idempotent(self):
        status = finalize_script.finalize(self.root, "financials", write=False)
        self.assertEqual(status, 1)
        self.assertNotIn("output_sha256", self.project.manifest()["files"][0])
        status = finalize_script.finalize(self.root, "financials", write=True)
        self.assertEqual(status, 0)
        manifest = self.project.manifest()
        for entry in manifest["files"]:
            digest = finalize_script.sha256(Path(entry["path"]))
            self.assertEqual(entry["output_sha256"], digest)
        self.assertIn("registration_finalized", manifest)
        self.assertEqual(finalize_script.finalize(self.root, "financials", write=True), 0)
        self.assertEqual(finalize_script.finalize(self.root, "financials", write=False), 0)

    def test_row_and_source_disagreements_refuse_to_write(self):
        status = finalize_script.finalize(self.root, "financials", write=True)
        self.assertEqual(status, 0)
        manifest = self.project.manifest()
        manifest["files"][0]["rows"] = 99
        self.project.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        self.assertEqual(finalize_script.finalize(self.root, "financials", write=True), 2)
        # The refused run leaves the registered hash of the other file intact.
        self.assertEqual(self.project.manifest()["files"][1]["output_sha256"],
                         finalize_script.sha256(Path(self.project.manifest()["files"][1]["path"])))

    def test_action_samples_events_file_is_registered(self):
        events_dir = self.root / "data/external/actions/run1"
        source_dir = self.root / "data/external/actions/raw"
        source_dir.mkdir(parents=True, exist_ok=True)
        rows = []
        for index in range(210):
            source = source_dir / f"page_{index:04d}.json"
            source.write_text(json.dumps({"index": index}), encoding="utf-8")
            rows.append({"symbol": f"{index:06d}", "ex_date": date(2024, 6, 20),
                         "source_file": str(source), "source_sha256": finalize_script.sha256(source)})
        write_parquet(events_dir / "events.parquet", rows)
        (events_dir / "manifest.json").write_text(json.dumps(
            {"rows": len(rows), "output": str(events_dir), "quarantined_events": 0,
             "errors": []}), encoding="utf-8")
        config = json.loads((self.root / "config/datasets.json").read_text(encoding="utf-8"))
        config["datasets"]["action_samples"] = {"manifest": "data/external/actions/run1/manifest.json"}
        (self.root / "config/datasets.json").write_text(json.dumps(config), encoding="utf-8")
        self.assertEqual(finalize_script.finalize(self.root, "action_samples", write=True), 0)
        manifest = json.loads((events_dir / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(len(manifest["files"]), 1)
        self.assertEqual(manifest["files"][0]["rows"], len(rows))
        self.assertEqual(manifest["files"][0]["per_event_sources_hash_checked"], 200)
        self.assertEqual(finalize_script.finalize(self.root, "action_samples", write=True), 0)
        # A per-event source changed after registration must be caught.
        rows[0]["source_sha256"] = "0" * 64
        write_parquet(events_dir / "events.parquet", rows)
        manifest["files"] = []
        (events_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        self.assertEqual(finalize_script.finalize(self.root, "action_samples", write=True), 2)


@unittest.skipUnless(PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON)
class FinancialAvailabilityEvidenceTests(unittest.TestCase):
    def setUp(self):
        temporary_root = PROJECT / ".test-tmp"
        temporary_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=temporary_root)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)

    def build_once(self, **kwargs) -> tuple[dict, Path]:
        project = SyntheticProject(self.root, **kwargs)
        manifest, manifest_path = availability_script.build(self.root)
        return manifest, manifest_path

    def test_matching_sample_records_every_comparison(self):
        manifest, manifest_path = self.build_once()
        self.assertEqual(manifest["original_notice_dates_compared"], 2)
        self.assertEqual(manifest["original_notice_dates_matched"], 1)
        self.assertEqual(len(manifest["mismatches"]), 1)
        self.assertIn("000002", manifest["mismatches"][0]["symbol"])
        self.assertTrue(manifest_path.is_file())
        sample = [json.loads(line) for line in
                  (manifest_path.parent / "sample_comparison.jsonl").read_text(encoding="utf-8").splitlines()]
        self.assertEqual(len(sample), 2)
        self.assertEqual(sample[0]["original_notice_date"], "2026-04-20")
        # Registering adds the dataset entry exactly once.
        config_path = availability_script.register(self.root, manifest_path)
        config = json.loads(config_path.read_text(encoding="utf-8"))
        self.assertIn("financial_availability", config["datasets"])
        before = config_path.read_text(encoding="utf-8")
        availability_script.register(self.root, manifest_path)
        self.assertEqual(config_path.read_text(encoding="utf-8"), before)

    def test_a_wholly_consistent_run_matches_every_row(self):
        project = SyntheticProject(self.root, corrupted_notice="2026-04-20 00:00:00")
        manifest, _ = availability_script.build(self.root)
        self.assertEqual(manifest["original_notice_dates_matched"], 2)
        self.assertEqual(manifest["mismatches"], [])

    def test_a_missing_raw_capture_is_recorded_as_unresolved(self):
        project = SyntheticProject(self.root)
        capture = Path(project.entries[1]["source_original_file"])
        capture.unlink()
        manifest, _ = availability_script.build(self.root)
        self.assertEqual(manifest["original_notice_dates_compared"], 1)
        self.assertEqual(len(manifest["unresolved_rows"]), 1)


if __name__ == "__main__":
    unittest.main()
