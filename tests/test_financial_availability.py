"""Availability dates decide lookahead safety, so the date logic is pinned."""
import importlib.util
from datetime import date, datetime, timezone
from pathlib import Path
import unittest

PROJECT = Path(__file__).resolve().parents[1]

try:
    SPEC = importlib.util.spec_from_file_location(
        "build_financial_availability", PROJECT / "scripts/build_financial_availability.py")
    builder = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(builder)
except ImportError as exc:
    builder = None
    SKIP_REASON = "optional dependency unavailable: " + str(exc)
else:
    SKIP_REASON = ""


@unittest.skipUnless(builder is not None, SKIP_REASON)
class FinancialAvailabilityTests(unittest.TestCase):
    def test_report_titles_map_to_their_period_end(self):
        cases = {"2026年半年度报告": "2026-06-30", "2026年一季度报告": "2026-03-31",
                 "2025年三季度报告": "2025-09-30", "2025年年度报告摘要": "2025-12-31",
                 "2018年第一季度报告正文": "2018-03-31"}
        for title, period in cases.items():
            with self.subTest(title=title):
                self.assertEqual(builder.report_period(title), period)

    def test_supplementary_and_correction_notices_are_not_periods(self):
        for title in ("2017年年度报告补充公告", "2020年年度报告更正公告"):
            with self.subTest(title=title):
                self.assertIsNone(builder.report_period(title))

    def test_announcement_time_is_converted_to_beijing_days(self):
        # A late-evening UTC timestamp already belongs to the next Beijing day.
        midnight = int(datetime(2018, 3, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        evening = int(datetime(2018, 3, 14, 17, 0, tzinfo=timezone.utc).timestamp() * 1000)
        self.assertEqual(builder.beijing_day(midnight), "2018-03-14")
        self.assertEqual(builder.beijing_day(evening), "2018-03-15")

    def test_update_classification_separates_comparables_from_unexplained(self):
        notice, following = "2025-08-23", "2026-08-15"
        self.assertEqual(builder.classify_update(notice, notice, following), "unrevised")
        self.assertEqual(builder.classify_update(notice, following, following), "next_year_comparable")
        self.assertEqual(builder.classify_update(notice, "2025-09-01", following), "unexplained")
        self.assertEqual(builder.classify_update(None, following, following), "unknown")

    def test_plus_year_handles_a_leap_day(self):
        self.assertEqual(builder.plus_year(date(2024, 2, 29)), date(2025, 2, 28))
        self.assertEqual(builder.plus_year(date(2025, 6, 30)), date(2026, 6, 30))


if __name__ == "__main__":
    unittest.main()
