"""Offline regression tests for immutable collection evidence and explicit cutoffs."""
from contextlib import redirect_stdout
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "scripts"))
sys.path.insert(0, str(PROJECT / "tests"))
from parquet_support import PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON, pa, pd, pq

try:
    import fetch_http_reference as http
    import fetch_tdx_reference as tdx
    import fetch_etf_daily as etf
    import fetch_financial_samples as financial
    import verify_local_source as verify
except ImportError as exc:
    AVAILABLE, SKIP_REASON = False, "optional dependency unavailable: " + str(exc)
else:
    AVAILABLE, SKIP_REASON = PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON


def response(payload=None, text=None, status=200):
    mocked = Mock()
    mocked.status_code = status
    mocked.text = text if text is not None else json.dumps(payload)
    if status >= 400:
        mocked.raise_for_status.side_effect = RuntimeError(f"HTTP {status}")
    return mocked


@unittest.skipUnless(AVAILABLE, SKIP_REASON)
class AcquisitionProvenanceTests(unittest.TestCase):
    def temporary_project(self):
        directory = tempfile.TemporaryDirectory(dir=PROJECT / "tests")
        self.addCleanup(directory.cleanup)
        return Path(directory.name)

    def quote_response(self, _url, params, timeout):
        symbol, _, start, _, _, adjustment = params["param"].split(",")
        key = "qfqday" if adjustment else "day"
        return response({"code": 0, "extra_provider_field": "retain me",
                         "data": {symbol: {key: [[start, "10", "10", "10", "10", "100"]]}}})

    def test_reference_repeated_and_failed_runs_preserve_previous_evidence(self):
        project = self.temporary_project()
        base = project / "reports" / "source-verification"
        base.mkdir(parents=True)
        legacy = base / "http-reference.json"
        legacy.write_bytes(b"existing evidence")
        session = Mock()
        session.get.side_effect = self.quote_response
        with patch.object(http, "PROJECT", project), patch.object(http.requests, "Session", return_value=session), redirect_stdout(io.StringIO()):
            first = http.main(["--as-of", "2026-10-09"])
            original = {p.name: p.read_bytes() for p in first.rglob("*.json")}
            second = http.main(["--as-of", "2026-10-09"])
            session.get.side_effect = None
            session.get.return_value = response({"code": -1, "msg": "source failed", "data": {}})
            with self.assertRaises(RuntimeError):
                http.main(["--as-of", "2026-10-09"])
        self.assertNotEqual(first, second)
        self.assertEqual(legacy.read_bytes(), b"existing evidence")
        self.assertEqual(original, {p.name: p.read_bytes() for p in first.rglob("*.json")})
        self.assertEqual(len(list(base.glob("http_*/http-reference.json"))), 3)
        saved = json.loads(next((first / "raw").glob("*.json")).read_text(encoding="utf-8"))
        self.assertEqual(json.loads(saved["response_text"])["extra_provider_field"], "retain me")

    def test_etf_full_response_is_saved_before_filtering_and_cannot_be_overwritten(self):
        project = self.temporary_project()
        raw = project / "etf-response.json"
        payload = {"code": 0, "server_metadata": {"version": "v2"}, "data": {"sh510300": {"day": [
            ["2017-12-29", "10", "10", "10", "10", "1"],
            ["2018-01-02", "10", "10", "10", "10", "2"],
            ["2019-01-02", "10", "10", "10", "10", "3"]]}}}
        with patch.object(etf.requests, "get", return_value=response(payload)):
            selected = etf.request_year("sh510300", 2018, "2026-10-09", raw)
            with self.assertRaises(FileExistsError):
                etf.request_year("sh510300", 2018, "2026-10-09", raw)
        self.assertEqual([row[0] for row in selected["rows"]], ["2018-01-02"])
        self.assertEqual(json.loads(json.loads(raw.read_text(encoding="utf-8"))["response_text"]), payload)

    def test_failed_http_and_non_json_responses_still_have_raw_evidence(self):
        project = self.temporary_project()
        for filename, returned, error in [
            ("failed.json", response(text="<html>rate limited</html>", status=514), RuntimeError),
            ("malformed.json", response(text="not JSON"), json.JSONDecodeError),
            ("empty.json", response({"code": 0, "data": {}}), ValueError)]:
            raw = project / filename
            with self.subTest(filename=filename), patch.object(etf.requests, "get", return_value=returned):
                with self.assertRaises(error):
                    etf.request_year("sh510300", 2018, "2026-10-09", raw)
            self.assertEqual(json.loads(raw.read_text(encoding="utf-8"))["response_text"], returned.text)

    def write_financial_cache(self, project, retrieved_at=None):
        snapshot = project / "source"
        (snapshot / "raw").mkdir(parents=True)
        records = [
            {"SECURITY_CODE": "000001", "REPORT_DATE": "2026-03-31 00:00:00", "NOTICE_DATE": "2026-05-01 00:00:00", "EPSJB": 1.0},
            {"SECURITY_CODE": "000001", "REPORT_DATE": "2026-06-30 00:00:00", "NOTICE_DATE": "2026-10-08 00:00:00", "EPSJB": 2.0}]
        envelope = {"params": {"reportName": "RPT_F10_FINANCE_MAINFINADATA"},
                    "payload": {"success": True, "result": {"count": 2, "data": records}}}
        if retrieved_at is not None:
            envelope["retrieved_at"] = retrieved_at
        raw = snapshot / "raw" / "000001.json"
        raw.write_text(json.dumps(envelope, indent=2), encoding="utf-8")
        return snapshot, raw

    def test_financial_cache_preserves_known_retrieval_time_and_explicit_cutoff(self):
        project = self.temporary_project()
        known = "2026-10-08T12:34:56+00:00"
        snapshot, raw = self.write_financial_cache(project, known)
        original = raw.read_bytes()
        with patch.object(financial, "PROJECT", project), patch.object(financial.requests, "Session") as networking, redirect_stdout(io.StringIO()):
            early = financial.main(["--cached-snapshot", str(snapshot), "--symbols", "000001", "--as-of", "2026-10-07"])
            later = financial.main(["--cached-snapshot", str(snapshot), "--symbols", "000001", "--as-of", "2026-10-09"])
        networking.assert_not_called()
        self.assertEqual(raw.read_bytes(), original)
        self.assertEqual((later / "raw" / raw.name).read_bytes(), original)
        self.assertEqual(pq.read_table(early / "000001_financials.parquet").num_rows, 1)
        frame = pq.read_table(later / "000001_financials.parquet").to_pandas()
        self.assertEqual(len(frame), 2)
        self.assertEqual(frame.source_retrieved_at.tolist(), [known, known])
        self.assertEqual(frame.snapshot_retrieved_at.tolist(), [known, known])
        self.assertFalse(frame.source_retrieval_time_unknown.any())
        self.assertTrue((frame.normalized_at != known).all())
        manifest = json.loads((later / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["as_of"], "2026-10-09")
        self.assertEqual(manifest["files"][0]["source_sha256"], hashlib.sha256(original).hexdigest())
        self.assertFalse(manifest["backtest_ready"])

    def test_legacy_financial_cache_has_unknown_source_time(self):
        project = self.temporary_project()
        snapshot, _ = self.write_financial_cache(project)
        with patch.object(financial, "PROJECT", project), redirect_stdout(io.StringIO()):
            output = financial.main(["--cached-snapshot", str(snapshot), "--symbols", "000001", "--as-of", "2026-10-09"])
        frame = pq.read_table(output / "000001_financials.parquet").to_pandas()
        self.assertTrue(frame.source_retrieved_at.isna().all())
        self.assertTrue(frame.snapshot_retrieved_at.isna().all())
        self.assertTrue(frame.source_retrieval_time_unknown.all())
        self.assertTrue(frame.normalized_at.notna().all())

    def test_new_complete_financial_response_is_accepted_as_cache(self):
        project = self.temporary_project()
        snapshot, raw = self.write_financial_cache(project)
        envelope = json.loads(raw.read_text(encoding="utf-8"))
        payload = envelope.pop("payload")
        envelope["response_text"] = json.dumps(payload)
        envelope["retrieved_at"] = "2026-10-08T10:00:00+00:00"
        raw.write_text(json.dumps(envelope), encoding="utf-8")
        _, cached_payload, collected = financial.cached_response(raw)
        self.assertEqual(cached_payload, payload)
        self.assertEqual(collected, envelope["retrieved_at"])

    def test_financial_wrong_security_is_rejected_with_original_response_preserved(self):
        project = self.temporary_project()
        snapshot, raw = self.write_financial_cache(project)
        envelope = json.loads(raw.read_text(encoding="utf-8"))
        envelope["payload"]["result"]["data"][0]["SECURITY_CODE"] = "600000"
        raw.write_text(json.dumps(envelope), encoding="utf-8")
        original = raw.read_bytes()
        with patch.object(financial, "PROJECT", project), redirect_stdout(io.StringIO()):
            with self.assertRaises(RuntimeError):
                financial.main(["--cached-snapshot", str(snapshot), "--symbols", "000001", "--as-of", "2026-10-09"])
        output = next((project / "data" / "external" / "financials").iterdir())
        manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(raw.read_bytes(), original)
        self.assertEqual((output / "raw" / raw.name).read_bytes(), original)
        self.assertFalse((output / "000001_financials.parquet").exists())
        self.assertFalse(manifest["complete_sample"])
        self.assertIn("security other than requested 000001", manifest["errors"][0]["error"])

    def test_financial_out_of_window_reports_are_rejected_even_with_unknown_notice(self):
        for invalid_period in ["2017-12-31 00:00:00", "2026-12-31 00:00:00", None]:
            with self.subTest(report_period=invalid_period):
                project = self.temporary_project()
                snapshot, raw = self.write_financial_cache(project)
                envelope = json.loads(raw.read_text(encoding="utf-8"))
                row = envelope["payload"]["result"]["data"][0]
                row["REPORT_DATE"], row["NOTICE_DATE"] = invalid_period, None
                raw.write_text(json.dumps(envelope), encoding="utf-8")
                original = raw.read_bytes()
                with patch.object(financial, "PROJECT", project), redirect_stdout(io.StringIO()):
                    with self.assertRaises(RuntimeError):
                        financial.main(["--cached-snapshot", str(snapshot), "--symbols", "000001", "--as-of", "2026-10-09"])
                output = next((project / "data" / "external" / "financials").iterdir())
                manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
                self.assertEqual(raw.read_bytes(), original)
                self.assertEqual((output / "raw" / raw.name).read_bytes(), original)
                self.assertFalse((output / "000001_financials.parquet").exists())
                self.assertEqual(manifest["files"], [])
                self.assertIn("outside the requested", manifest["errors"][0]["error"])

    def test_tdx_versions_and_preserves_bars_when_actions_fail(self):
        project = self.temporary_project()
        base = project / "reports" / "source-verification"
        base.mkdir(parents=True)
        legacy = base / "tdx-reference.json"
        legacy.write_bytes(b"existing TDX evidence")
        api = Mock()
        api.connect.return_value = True
        api.get_security_bars.return_value = [{"year": 2026, "month": 10, "day": 9}, {"year": 2026, "month": 10, "day": 10}]
        api.get_xdxr_info.return_value = []
        fake_hq = types.ModuleType("pytdx.hq")
        fake_hq.TdxHq_API = Mock(return_value=api)
        fake_pytdx = types.ModuleType("pytdx")
        fake_pytdx.hq = fake_hq
        with patch.dict(sys.modules, {"pytdx": fake_pytdx, "pytdx.hq": fake_hq}), patch.object(tdx, "PROJECT", project), redirect_stdout(io.StringIO()):
            first = tdx.main(["--as-of", "2026-10-09"])
            before = (first / "tdx-reference.json").read_bytes()
            api.get_xdxr_info.side_effect = RuntimeError("action endpoint failed")
            with self.assertRaises(RuntimeError):
                tdx.main(["--as-of", "2026-10-09"])
        self.assertEqual(legacy.read_bytes(), b"existing TDX evidence")
        self.assertEqual((first / "tdx-reference.json").read_bytes(), before)
        result = json.loads(before)
        self.assertEqual(len(result["securities"]["000001"]["daily_bars"]), 1)
        self.assertEqual(len(json.loads((first / "raw" / "000001_daily.json").read_text(encoding="utf-8"))["daily_bars"]), 2)
        failed = next(p for p in base.glob("tdx_*") if p != first)
        self.assertTrue((failed / "raw" / "000001_daily.json").exists())

    def test_verification_supports_legacy_and_explicit_versions_without_overwriting(self):
        project = self.temporary_project()
        base = project / "reports" / "source-verification"
        base.mkdir(parents=True)
        old_verification = base / "verification.json"
        old_verification.write_bytes(b"existing verification")
        stocks = project / "stocks"
        stocks.mkdir()
        row = ["2026-07-07", "10", "10", "10", "10", "100"]
        symbols = {symbol: {key: {"bars": [row]} for key in ("day", "qfqday")} for symbol in http.SYMBOLS}
        reference = {"errors": [], "start": "2026-07-07", "end": "2026-07-07", "securities": symbols}
        legacy = base / "http-reference.json"
        legacy.write_text(json.dumps(reference), encoding="utf-8")
        for symbol in symbols:
            frame = pd.DataFrame({"time": [1783436], "open": [10.], "high": [10.], "low": [10.], "close": [10.], "tick_volume": [100]})
            pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), stocks / f"{symbol[2:]}_daily.parquet")
        version = base / "versioned-reference.json"
        version.write_bytes(legacy.read_bytes())
        with patch.object(verify, "PROJECT", project), redirect_stdout(io.StringIO()):
            first = verify.main(["--source", str(stocks), "--as-of", "2026-10-09"])
            second = verify.main(["--source", str(stocks), "--reference", str(version), "--as-of", "2026-10-09"])
        self.assertNotEqual(first, second)
        self.assertEqual(old_verification.read_bytes(), b"existing verification")
        summary = json.loads((second / "verification.json").read_text(encoding="utf-8"))
        self.assertEqual(summary["reference"], str(version.resolve()))
        self.assertEqual(summary["verified_at"], "2026-10-09")
        self.assertTrue(summary["sample_check_passed"])

    def test_default_collection_date_uses_china_timezone(self):
        instant = datetime(2026, 10, 8, 16, 1, tzinfo=timezone.utc)
        with patch.object(http, "datetime") as mocked:
            mocked.now.side_effect = lambda tz: instant.astimezone(tz)
            self.assertEqual(http.china_today(), "2026-10-09")


if __name__ == "__main__":
    unittest.main()
