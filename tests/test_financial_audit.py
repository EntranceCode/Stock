"""Meaningful checks for snapshot audit coverage and prohibited TTM shortcuts."""
from pathlib import Path
import sys
import unittest

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "scripts"))
sys.path.insert(0, str(PROJECT / ".deps" / "parquet"))
import pandas as pd
from audit_financial_snapshot import audit_frame, numeric_stats, ttm_coverage


class FinancialAuditTests(unittest.TestCase):
    def sample(self):
        periods = ["2024-03-31", "2024-12-31", "2025-03-31", "2025-06-30"]
        frame = pd.DataFrame({"SECURITY_CODE": ["000001"] * 4, "REPORT_DATE": periods,
                              "NOTICE_DATE": ["2024-04-25", "2025-03-10", "2025-04-25", None],
                              "UPDATE_DATE": ["2025-01-01", "2025-03-10", "2025-04-24", None],
                              "CURRENCY": ["CNY"] * 4, "EPSJB": [-.1, 1., 0., .2],
                              "BPS": [1., -1., 0., None], "PARENTNETPROFIT": [-10., 100., 20., 30.]})
        frame["report_period"] = pd.to_datetime(frame.REPORT_DATE).dt.date
        notice = pd.to_datetime(frame.NOTICE_DATE)
        frame["announcement_date"] = notice.dt.date
        frame["earliest_candidate_available_date"] = (notice + pd.Timedelta(days=1)).dt.date
        frame["original_vintage_verified"] = False
        return frame

    def test_missing_invalid_and_infinite_values_are_distinct(self):
        stats = numeric_stats(pd.Series([None, "bad", float("inf"), float("-inf"), -2, 0, 3]))
        self.assertEqual(stats["missing"], 1)
        self.assertEqual(stats["invalid_nonmissing"], 1)
        self.assertEqual(stats["nonfinite_nonmissing"], 2)
        self.assertEqual((stats["negative"], stats["zero"], stats["positive"]), (1, 1, 1))

    def test_update_relation_does_not_assert_a_revision(self):
        audit = audit_frame(self.sample(), "2026-10-07")
        relations = audit["date_relationships"]
        self.assertEqual(relations["update_after_notice"], 1)
        self.assertEqual(relations["update_before_notice"], 1)
        self.assertEqual(relations["update_same_as_notice"], 1)
        self.assertEqual(relations["update_notice_relation_unknown"], 1)
        self.assertNotIn("confirmed_revision", relations)
        self.assertEqual(audit["original_vintage_verified_true"], 0)

    def test_missing_announcements_do_not_create_candidate_dates(self):
        audit = audit_frame(self.sample(), "2026-10-07")
        self.assertEqual(audit["date_quality"]["NOTICE_DATE"]["missing"], 1)
        self.assertEqual(audit["date_relationships"]["candidate_date_present_without_notice"], 0)

    def test_ttm_requires_previous_annual_and_same_interim_total(self):
        coverage = ttm_coverage(self.sample())
        counts = coverage["PARENTNETPROFIT"]
        self.assertEqual(counts["annual_arithmetic_candidate"], 1)
        self.assertEqual(counts["interim_arithmetic_candidate"], 1)
        self.assertEqual(counts["missing_required_report"], 2)
        self.assertNotIn("EPSJB", coverage)
        self.assertNotIn("ROEJQ", coverage)
        self.assertNotIn("MGJYXJJE", coverage)

    def test_currency_difference_blocks_ttm_candidate(self):
        frame = self.sample()
        frame.loc[0, "CURRENCY"] = "HKD"
        counts = ttm_coverage(frame)["PARENTNETPROFIT"]
        self.assertEqual(counts["interim_arithmetic_candidate"], 0)
        self.assertEqual(counts["currency_mismatch"], 1)

    def test_duplicate_report_periods_are_never_silently_deduplicated(self):
        frame = pd.concat([self.sample(), self.sample().iloc[[0]]], ignore_index=True)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            ttm_coverage(frame)

    def test_nonpositive_valuation_inputs_are_recorded_as_values(self):
        audit = audit_frame(self.sample(), "2026-10-07")
        self.assertEqual(audit["fields"]["EPSJB"]["negative"], 1)
        self.assertEqual(audit["fields"]["EPSJB"]["zero"], 1)
        self.assertEqual(audit["fields"]["BPS"]["negative"], 1)
        self.assertEqual(audit["fields"]["BPS"]["missing"], 1)

    def test_absent_numeric_fields_are_missing_not_filled_with_zero(self):
        frame = self.sample().drop(columns=["EPSJB", "BPS"])
        audit = audit_frame(frame, "2026-10-07")
        self.assertFalse(audit["fields"]["EPSJB"]["present_in_loaded_frame"])
        self.assertEqual(audit["fields"]["EPSJB"]["missing"], len(frame))
        self.assertEqual(audit["fields"]["EPSJB"]["zero"], 0)


if __name__ == "__main__":
    unittest.main()
