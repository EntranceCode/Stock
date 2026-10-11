import importlib.util
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("history_merge", Path(__file__).resolve().parents[1] / "scripts/merge_stock_history.py")
merge = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(merge)


class HistoryMergeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def entry(self, symbol, suffix=""):
        path = self.root / (symbol + suffix + ".parquet")
        path.write_bytes(b"fixed evidence" + suffix.encode())
        return {"symbol": symbol, "rows": 1, "start": "2024-01-01", "end": "2024-01-01", "output": str(path), "output_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}

    def manifest(self, name, entries, **kwargs):
        path = self.root / (name + ".json")
        path.write_text(json.dumps({"complete": True, "files": entries, "readiness_gaps": ["unverified_history"], **kwargs}), encoding="utf-8")
        return path

    def test_existing_symbols_are_retained_and_source_failures_stay_visible(self):
        original = self.entry("000001")
        base = self.manifest("base", [original])
        extra = self.manifest("extra", [self.entry("000001", "new"), self.entry("000005")], complete=False, errors=[{"symbol":"000018","error":"empty"}])
        result = merge.inventory(base, [extra], self.root)
        self.assertEqual(result["files"][0]["output"], original["output"])
        self.assertEqual(result["normalized_rows"], 2)
        self.assertEqual(len(result["source_error_history"]), 1)
        self.assertEqual(len(result["kept_original_duplicates"]), 1)

    def test_multilisting_history_is_preserved_in_source_but_withheld_from_price_view(self):
        entry = self.entry("000005")
        entry["source_request"] = {"multiple_listing_records_require_review": True}
        result = merge.inventory(self.manifest("base", [self.entry("000001")]), [self.manifest("extra", [entry])], self.root)
        self.assertEqual(len(result["files"]), 1)
        self.assertEqual(result["withheld_multiple_listing_symbols"][0]["symbol"], "000005")
        self.assertTrue(Path(entry["output"]).exists())

    def test_tampered_and_duplicate_evidence_is_refused(self):
        entry = self.entry("000001")
        path = self.manifest("base", [entry])
        Path(entry["output"]).write_bytes(b"tampered")
        with self.assertRaisesRegex(ValueError,"SHA-256"):
            merge.inventory(path, [], self.root)
        entry = self.entry("000001")
        with self.assertRaisesRegex(ValueError,"duplicate"):
            merge.inventory(self.manifest("dupe", [entry,entry]), [], self.root)


if __name__ == "__main__":
    unittest.main()
