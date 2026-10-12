from datetime import date
from pathlib import Path
import sys
import unittest

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "scripts"))
sys.path.insert(0, str(PROJECT / "tests"))
from parquet_support import PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON, pa, pd, pq

try:
    from normalize_security_references import security_code, source_date, name_marker, listing_record, name_event
except ImportError as exc:
    AVAILABLE, SKIP_REASON = False, "optional dependency unavailable: " + str(exc)
else:
    AVAILABLE, SKIP_REASON = PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON


@unittest.skipUnless(AVAILABLE, SKIP_REASON)
class SecurityReferenceTests(unittest.TestCase):
    def test_codes_keep_leading_zeros_and_reject_implicit_repair(self):
        self.assertEqual(security_code("000001"), "000001")
        for value in (1, "1.0", "SZ000001", "000001.0"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                security_code(value)

    def test_source_date_formats_and_bounds(self):
        self.assertEqual(source_date("19980122", date(2026, 10, 9)), date(1998, 1, 22))
        for value in ("2026-10-10", "2025-02-30", "nan", "1989-12-31"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                source_date(value, date(2026, 10, 9))

    def test_listing_dates_are_not_tradability_or_verified_boundaries(self):
        row = {"证券代码": "000004", "证券简称": "国华退", "上市日期": "1990-12-01", "终止上市日期": "2026-07-14"}
        record = listing_record(row, "证券代码", "证券简称", "上市日期", "终止上市日期", "SZSE", date(2026, 10, 9))
        self.assertFalse(record["listed_at_snapshot"])
        self.assertFalse(record["termination_boundary_verified"])
        self.assertNotIn("tradable", record)
        row["终止上市日期"] = "1990-11-30"
        with self.assertRaises(ValueError):
            listing_record(row, "证券代码", "证券简称", "上市日期", "终止上市日期", "SZSE", date(2026, 10, 9))

    def test_historical_name_does_not_use_current_display_name(self):
        row = {"证券代码": "000403", "证券简称": "派林生物", "变更日期": "2018-11-22", "变更前简称": "ST生化", "变更后简称": "振兴生化"}
        event = name_event(row, date(2026, 10, 9))
        self.assertEqual(event["name_before"], "ST生化")
        self.assertEqual(event["name_after"], "振兴生化")
        self.assertEqual(event["name_marker_before"], "ST_name")
        self.assertEqual(event["name_marker_after"], "unmarked_name_not_proof_of_normal_status")
        self.assertFalse(event["regulatory_status_verified"])

    def test_missing_name_cannot_become_normal_status(self):
        self.assertEqual(name_marker(""), "unknown")
        self.assertEqual(name_marker(None), "unknown")
        self.assertEqual(name_marker("＊ＳＴ 样本"), "star_ST_name")
        self.assertEqual(name_marker("PT样本"), "PT_name")
        with self.assertRaises(ValueError):
            name_event({"证券代码": "000001", "证券简称": "当前名", "变更日期": "2020-01-01", "变更前简称": "", "变更后简称": "新名"}, date(2026, 10, 9))


if __name__ == "__main__":
    unittest.main()
