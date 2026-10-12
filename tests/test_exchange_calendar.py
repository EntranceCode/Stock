from copy import deepcopy
from datetime import date
import json
from pathlib import Path
import sys
import unittest

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "scripts"))
sys.path.insert(0, str(PROJECT / "tests"))
from parquet_support import PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON, pa, pd, pq

try:
    from build_exchange_calendar import ExchangeCalendar, CalendarCoverageError
except ImportError as exc:
    AVAILABLE, SKIP_REASON = False, "optional dependency unavailable: " + str(exc)
else:
    AVAILABLE, SKIP_REASON = PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON


@unittest.skipUnless(AVAILABLE, SKIP_REASON)
class CalendarTests(unittest.TestCase):
    def setUp(self):
        self.catalog = json.loads((PROJECT / "config/exchange-calendar-sources.json").read_text(encoding="utf-8"))
        self.calendar = ExchangeCalendar(self.catalog)

    def test_2019_amendment_is_effective_only_after_publication(self):
        day = date(2019, 5, 2)
        self.assertTrue(self.calendar.is_scheduled_open(day, knowledge_as_of=date(2019, 4, 17)))
        self.assertFalse(self.calendar.is_scheduled_open(day, knowledge_as_of=date(2019, 4, 18)))
        self.assertEqual(self.calendar.next_session_after(date(2019, 4, 30)), date(2019, 5, 6))

    def test_2020_special_extension_replaces_weekday_guess(self):
        day = date(2020, 1, 31)
        self.assertTrue(self.calendar.is_scheduled_open(day, knowledge_as_of=date(2020, 1, 26)))
        self.assertFalse(self.calendar.is_scheduled_open(day, knowledge_as_of=date(2020, 1, 27)))
        self.assertEqual(self.calendar.next_session_after(date(2020, 1, 23)), date(2020, 2, 3))

    def test_government_makeup_weekend_does_not_become_exchange_session(self):
        self.assertFalse(self.calendar.is_scheduled_open(date(2026, 10, 10)))
        self.assertEqual(self.calendar.next_session_after(date(2026, 9, 30)), date(2026, 10, 8))

    def test_new_year_notice_can_close_previous_year_end(self):
        day = date(2018, 12, 31)
        self.assertTrue(self.calendar.is_scheduled_open(day, knowledge_as_of=date(2018, 12, 19)))
        self.assertFalse(self.calendar.is_scheduled_open(day, knowledge_as_of=date(2018, 12, 20)))
        self.assertEqual(self.calendar.next_session_after(date(2018, 12, 28)), date(2019, 1, 2))

    def test_scope_and_exchange_must_be_verified(self):
        for day, exchange in [(date(1991, 1, 5), "SSE"), (date(2026, 10, 9), "SZSE"), (date(2027, 1, 4), "SSE")]:
            with self.subTest(day=day, exchange=exchange), self.assertRaises(CalendarCoverageError):
                self.calendar.is_scheduled_open(day, exchange)
        with self.assertRaises(CalendarCoverageError):
            self.calendar.next_session_after(date(2026, 12, 31))

    def test_unannounced_future_year_and_unknown_sources_are_rejected(self):
        with self.assertRaises(CalendarCoverageError):
            self.calendar.is_scheduled_open(date(2026, 1, 5), knowledge_as_of=date(2025, 12, 1))
        for changed in ("unverified", "missing_year", "reversed"):
            catalog = deepcopy(self.catalog)
            if changed == "unverified":
                catalog["years"][0]["verified"] = False
            elif changed == "missing_year":
                catalog["years"].pop()
            else:
                catalog["years"][0]["closed_intervals"][0][0] = "2019-01-01"
            with self.subTest(changed=changed), self.assertRaises(CalendarCoverageError):
                ExchangeCalendar(catalog)

    def test_szse_catalog_is_scoped_to_its_own_exchange(self):
        catalog = json.loads((PROJECT / "config/exchange-calendar-sources-szse.json").read_text(encoding="utf-8"))
        calendar = ExchangeCalendar(catalog)
        self.assertEqual(catalog["exchange"], "SZSE")
        self.assertTrue(calendar.is_scheduled_open(date(2021, 2, 8), "SZSE"))
        self.assertFalse(calendar.is_scheduled_open(date(2021, 2, 11), "SZSE"))
        with self.assertRaises(CalendarCoverageError):
            calendar.is_scheduled_open(date(2021, 2, 8), "SSE")

    def test_independently_sourced_szse_closures_match_the_sse_catalog(self):
        def closures(name):
            catalog = json.loads((PROJECT / "config" / name).read_text(encoding="utf-8"))
            return {item["year"]: sorted((a, b) for a, b, _ in item["closed_intervals"]) for item in catalog["years"]}
        self.assertEqual(closures("exchange-calendar-sources-szse.json"),
                         closures("exchange-calendar-sources.json"))


if __name__ == "__main__":
    unittest.main()
