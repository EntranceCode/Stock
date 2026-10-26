"""Session-gap attribution checks on a synthetic project tree.

The builder derives design §5.1 ``session_gap_records`` - symbol, dates and
cause - from the registered series, the registered calendars and the acquired
provider staging. The synthetic fixture exercises each cause the builder can
record and the undecidable case it must leave alone.
"""
from datetime import date
import importlib.util
import io
import json
import contextlib
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
sys.path.insert(0, str(PROJECT / "tests"))
from parquet_support import PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON, pa, pq  # noqa: E402


def load_script(name: str):
    spec = importlib.util.spec_from_file_location(name.replace(".", "_"), PROJECT / "scripts" / name)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = load_script("build_session_gap_records.py")

MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY = (date(2024, 1, 2), date(2024, 1, 3),
                                               date(2024, 1, 4), date(2024, 1, 5),
                                               date(2024, 1, 8))


def write_parquet(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def write_json(path: Path, value: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    return path


@unittest.skipUnless(PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON)
class SessionGapRecordTests(unittest.TestCase):
    def setUp(self):
        temporary_root = PROJECT / ".test-tmp"
        temporary_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=temporary_root)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.build_tree()

    def build_tree(self) -> None:
        root = self.root
        # Both exchange calendars: the week is scheduled open.
        for exchange, directory in (("SSE", "calendar"), ("SZSE", "calendar_szse")):
            rows = [{"calendar_date": day, "exchange": exchange, "is_scheduled_open": True}
                    for day in (MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY)]
            path = write_parquet(root / directory / "calendar.parquet", rows)
            write_json(root / directory / "manifest.json", {
                "calendar_source_scope_complete": True,
                "files": [{"path": str(path), "sha256": "0" * 64, "rows": len(rows)}]})
        # The registered series skips Tuesday and Thursday.
        series_path = write_parquet(root / "normalized/000001_daily.parquet",
                                    [{"trade_date": day, "close": 10.0}
                                     for day in (MONDAY, WEDNESDAY, FRIDAY)])
        write_json(root / "normalized/manifest.json", {
            "files": [{"symbol": "000001", "path": str(series_path), "output": str(series_path),
                       "rows": 3, "start": MONDAY.isoformat(), "end": FRIDAY.isoformat()}]})
        # Listing candidates fix the security's exchange.
        candidates = write_parquet(root / "metadata/listing_candidates.parquet",
                                   [{"symbol": "000001", "exchange": "SZSE",
                                     "source_listing_date": date(2000, 1, 3)}])
        write_json(root / "metadata/manifest.json", {
            "files": [{"path": str(candidates), "sha256": "0" * 64, "rows": 1}]})
        # Acquired staging: Tuesday is a provider halt, Thursday has no row at
        # all, and Friday carries an active row the series lacks.
        observations = root / "data/external/baostock_history/run1/observations"
        write_parquet(observations / "000001_observations.parquet",
                      [{"trade_date": MONDAY, "provider_trade_status": 1},
                       {"trade_date": TUESDAY, "provider_trade_status": 0},
                       {"trade_date": WEDNESDAY, "provider_trade_status": 1},
                       {"trade_date": FRIDAY, "provider_trade_status": 1}])
        write_json(root / "config/datasets.json", {"datasets": {
            "stock_daily": {"manifest": "normalized/manifest.json"},
            "sse_calendar": {"manifest": "calendar/manifest.json"},
            "szse_calendar": {"manifest": "calendar_szse/manifest.json"},
            "security_metadata": {"manifest": "metadata/manifest.json"}}})

    def run_builder(self, *args: str) -> dict:
        original = builder.PROJECT
        builder.PROJECT = self.root
        try:
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = builder.main(list(args))
        finally:
            builder.PROJECT = original
        text = output.getvalue()
        # The summary is printed first; a trailing write notice follows it.
        summary, _ = json.JSONDecoder().raw_decode(text[text.index("{"):])
        return {"code": code, "summary": summary, "stdout": text}

    def test_each_missing_session_is_attributed_to_its_evidence(self):
        result = self.run_builder()
        self.assertEqual(result["code"], 0)
        summary = result["summary"]["stock_daily"]
        self.assertEqual(summary["securities_with_gaps"], 1)
        self.assertEqual(summary["missing_sessions"], 2)
        self.assertEqual(summary["undecidable_securities"], 0)
        self.assertEqual(summary["by_cause"], {"provider_halt_observation": 1,
                                               "no_provider_observation": 1})
        # The active staging row for Friday is a session the series holds, so it
        # is not a gap; Tuesday and Thursday are the two recorded causes.

    def test_writing_records_puts_them_in_the_registered_manifest(self):
        self.run_builder("--write")
        manifest = json.loads((self.root / "normalized/manifest.json").read_text(encoding="utf-8"))
        records = manifest["session_gap_records"]
        by_cause = {record["cause"]: record for record in records}
        self.assertEqual(by_cause["provider_halt_observation"]["symbol"], "000001")
        self.assertEqual(by_cause["provider_halt_observation"]["dates"], [TUESDAY.isoformat()])
        self.assertEqual(by_cause["no_provider_observation"]["dates"], [THURSDAY.isoformat()])

    def test_a_security_without_staging_stays_undecidable(self):
        staging = self.root / "data/external/baostock_history/run1/observations/000001_observations.parquet"
        staging.unlink()
        result = self.run_builder()
        self.assertEqual(result["summary"]["stock_daily"]["undecidable_securities"], 1)
        self.assertEqual(result["summary"]["stock_daily"]["recorded"], 0)


if __name__ == "__main__":
    unittest.main()
