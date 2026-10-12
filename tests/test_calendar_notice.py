"""A calendar catalog is reviewed against notice text, so the parser is checked."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

PROJECT = Path(__file__).resolve().parents[1]

try:
    SPEC = importlib.util.spec_from_file_location(
        "extract_calendar_notice", PROJECT / "scripts/extract_calendar_notice.py")
    extractor = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(extractor)
except ImportError as exc:
    extractor = None
    SKIP_REASON = "optional dependency unavailable: " + str(exc)
else:
    SKIP_REASON = ""


@unittest.skipUnless(extractor is not None, SKIP_REASON)
class NoticeExtractionTests(unittest.TestCase):
    def test_inline_markup_spacing_inside_dates_is_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "notice.html"
            path.write_text("<p>202 4 年 1 月 1 日休市</p>", encoding="utf-8")
            self.assertEqual(extractor.notice_text(path), "2024年1月1日休市")

    def test_cross_year_range_single_day_and_trailing_weekend_note(self):
        body = ("（一）元旦：2023年12月30日（星期六）至2024年1月1日（星期一）休市，1月2日（星期二）起照常开市。"
                "（二）春节：2月9日（星期五）至2月17日（星期六）休市，2月19日（星期一）起照常开市。"
                "另外，2月4日（星期日）、2月18日（星期日）为周末休市。"
                "（三）端午节：6月10日（星期一）休市，6月11日（星期二）起照常开市。")
        text = "关于2024年部分节假日休市安排的通知一、休市安排" + body + "二、清算交收按中国结算安排。"
        self.assertEqual(extractor.intervals(text, 2024), [
            ("元旦", "2023-12-30", "2024-01-01"),
            ("春节", "2024-02-09", "2024-02-17"),
            ("端午节", "2024-06-10", "2024-06-10"),
        ])

    def test_notice_without_a_closure_section_is_rejected(self):
        with self.assertRaises(ValueError):
            extractor.intervals("关于2024年部分节假日安排的通知二、清算交收", 2024)

    def test_closure_item_without_a_date_is_rejected(self):
        with self.assertRaises(ValueError):
            extractor.intervals("一、休市安排（一）元旦：另行通知二、清算交收", 2024)


if __name__ == "__main__":
    unittest.main()
