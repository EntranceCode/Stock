"""Admission and evidence checks for the bounded official financial pilot."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "scripts"))
from fetch_financial_originals import Capture, classify_title, period_from_title, publication


def announcement(number="123", day="2019-03-29", symbol="600519"):
    epoch = int(datetime.fromisoformat(day).replace(tzinfo=timezone(timedelta(hours=8))).timestamp() * 1000)
    return {"secCode": symbol, "announcementId": number, "announcementTime": epoch,
            "announcementTitle": "2018年年度报告", "adjunctUrl": f"finalpage/{day}/{number}.PDF"}


class Response:
    def __init__(self, body, status=200):
        self.content = body
        self.status_code = status
        self.headers = {"Content-Type": "application/json"}
        self.url = "https://www.cninfo.com.cn/final"

    def json(self):
        return json.loads(self.content)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise ValueError("HTTP rejected")


class FinancialOriginalTests(unittest.TestCase):
    def temporary(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        return Path(directory.name)

    def test_midnight_epoch_is_only_a_date(self):
        result = publication(announcement())
        self.assertEqual(result["publication_date"], "2019-03-29")
        self.assertEqual(result["publication_time_precision"], "date_only")
        self.assertIsNone(result["available_at"])
        self.assertFalse(result["availability_verified"])

    def test_independent_epoch_and_path_dates_must_agree(self):
        row = announcement()
        row["adjunctUrl"] = "finalpage/2019-03-30/123.PDF"
        with self.assertRaises(ValueError):
            publication(row)
        row["announcementTime"] = "1553788800000"
        with self.assertRaises(ValueError):
            publication(row)

    def test_original_and_correction_are_not_conflated(self):
        self.assertEqual(classify_title("2018年年度报告"), "original_report_candidate")
        self.assertEqual(classify_title("2018年年度报告（修订版）"), "correction_or_supplement_candidate")
        self.assertEqual(classify_title("关于2019年第三季度报告的更正公告"), "correction_or_supplement_candidate")
        self.assertEqual(classify_title("2019年年度报告英文版"), "summary_or_translation")
        self.assertEqual(period_from_title("<em>2019</em>年第三季度报告全文"), "2019-09-30")
        self.assertIsNone(period_from_title("关于会计差错更正的公告"))

    def test_failed_response_is_saved_complete_before_validation(self):
        root = self.temporary()
        capture = Capture(root)
        body = b'{"error":"rate limit","details":[1,2,3]}'
        with patch("fetch_financial_originals.requests.request", return_value=Response(body, 429)):
            with self.assertRaises(ValueError):
                capture.request("rejection", "GET", "https://www.cninfo.com.cn/example")
        self.assertEqual((root / "raw/rejection.body").read_bytes(), body)
        evidence = json.loads((root / "raw/rejection.http.json").read_text(encoding="utf-8"))
        self.assertEqual(evidence["status_code"], 429)
        self.assertEqual(evidence["response_body_sha256"], hashlib.sha256(body).hexdigest())
        with patch("fetch_financial_originals.requests.request", return_value=Response(b"new")):
            with self.assertRaises(FileExistsError):
                capture.request("rejection", "GET", "https://www.cninfo.com.cn/example")
        self.assertEqual((root / "raw/rejection.body").read_bytes(), body)

    def test_page_count_mismatch_is_rejected(self):
        capture = Capture(self.temporary())
        payload = {"totalAnnouncement": 2, "announcements": [announcement()], "hasMore": False}
        with patch.object(capture, "request", return_value=(Response(json.dumps(payload).encode()), {"path": "mock", "sha256": "hash"})):
            with self.assertRaisesRegex(ValueError, "full announcement count"):
                capture.pages("600519", "official-id", "sse", "2018-01-01", "2026-10-07", "reports")

    def test_page_universe_and_duplicate_ids_are_rejected(self):
        capture = Capture(self.temporary())
        for rows in ([announcement(symbol="000001")], [announcement(), announcement()]):
            payload = {"totalAnnouncement": len(rows), "announcements": rows, "hasMore": False}
            with patch.object(capture, "request", return_value=(Response(json.dumps(payload).encode()), {"path": "mock", "sha256": "hash"})):
                with self.assertRaises(ValueError):
                    capture.pages("600519", "official-id", "sse", "2018-01-01", "2026-10-07", "reports")

    def test_changed_directory_count_between_pages_is_rejected(self):
        capture = Capture(self.temporary())
        first = {"totalAnnouncement": 2, "announcements": [announcement()], "hasMore": True}
        second = {"totalAnnouncement": 3, "announcements": [announcement("124")], "hasMore": False}
        evidence = {"path": "mock", "sha256": "hash"}
        with patch.object(capture, "request", side_effect=[(Response(json.dumps(x).encode()), evidence) for x in (first, second)]):
            with self.assertRaisesRegex(ValueError, "Directory changed"):
                capture.pages("600519", "official-id", "sse", "2018-01-01", "2026-10-07", "reports")

    def test_pdf_not_admitted_without_value_or_restatement_review(self):
        capture = Capture(self.temporary())
        row = announcement()
        row["title_clean"] = row["announcementTitle"]
        row["directory_evidence"] = {"path": "mock", "sha256": "hash"}
        row["associateAnnouncement"] = "456"
        with patch("fetch_financial_originals.requests.request", return_value=Response(b"%PDF-1.7\nexample")):
            result = capture.pdf(row)
        self.assertFalse(result["original_values_extracted_verified"])
        self.assertFalse(result["original_vintage_verified"])
        self.assertFalse(result["restatement_chain_verified"])
        self.assertEqual(result["associate_announcement_raw"], "456")
        self.assertIn("not assumed", result["association_semantics"])
        self.assertIsNone(result["available_at"])


if __name__ == "__main__":
    unittest.main()
