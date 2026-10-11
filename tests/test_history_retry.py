import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location("history_retry", SCRIPTS / "retry_baostock_history.py")
retry = importlib.util.module_from_spec(spec)
spec.loader.exec_module(retry)


class HistoryRetryTests(unittest.TestCase):
    def write_batch(self, root, name, requests, acquired):
        folder = root / name
        folder.mkdir()
        (folder / "requests.json").write_text(json.dumps(requests), encoding="utf-8")
        path = folder / "manifest.json"
        path.write_text(json.dumps({"files": [{"source_request": {"code": code}} for code in acquired]}), encoding="utf-8")
        return path

    def test_retry_includes_failed_and_unattempted_but_not_acquired(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_batch(Path(directory), "batch", [{"code": code} for code in ("sh.600001", "sh.600002", "sz.000005")], ["sh.600001"])
            requests, sources = retry.pending_requests([path])
            self.assertEqual([item["code"] for item in requests], ["sh.600002", "sz.000005"])
            self.assertEqual(len(sources), 2)

    def test_later_success_resolves_an_earlier_failed_request(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self.write_batch(root, "first", [{"code": "sh.600002"}], [])
            second = self.write_batch(root, "second", [{"code": "sh.600002"}], ["sh.600002"])
            self.assertEqual(retry.pending_requests([first, second])[0], [])

    def test_conflicting_request_windows_are_not_silently_merged(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self.write_batch(root, "first", [{"code": "sh.600002", "start": "2018-01-01"}], [])
            second = self.write_batch(root, "second", [{"code": "sh.600002", "start": "2020-01-01"}], [])
            with self.assertRaisesRegex(ValueError, "Conflicting"):
                retry.pending_requests([first, second])
