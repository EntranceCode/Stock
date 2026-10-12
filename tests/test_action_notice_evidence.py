"""Disclosure pages must be decoded before their text is trusted as evidence."""
import importlib.util
from pathlib import Path
import unittest

PROJECT = Path(__file__).resolve().parents[1]

try:
    SPEC = importlib.util.spec_from_file_location(
        "action_notice_evidence", PROJECT / "scripts/fetch_action_notice_evidence.py")
    collector = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(collector)
except ImportError as exc:
    collector = None
    SKIP_REASON = "optional dependency unavailable: " + str(exc)
else:
    SKIP_REASON = ""


class FakeResponse:
    def __init__(self, content, encoding):
        self.content = content
        self.encoding = encoding


@unittest.skipUnless(collector is not None, SKIP_REASON)
class NoticeTextTests(unittest.TestCase):
    def page(self, body):
        return FakeResponse(("<html><head><style>p{color:red}</style>"
                             "<script>var leaked=1;</script></head><body>" + body + "</body></html>").encode("utf-8"), "ISO-8859-1")

    def test_utf8_page_is_not_decoded_through_the_iso_8859_1_fallback(self):
        # Designated disclosure newspapers serve UTF-8 without declaring a charset,
        # so requests reports ISO-8859-1 and a naive decode yields mojibake.
        text = collector.notice_text(self.page("<p>流通股股东每股分配现金红利0.32091元</p>"))
        self.assertIn("流通股股东每股分配现金红利", text)
        self.assertIn("0.32091", text)

    def test_markup_script_and_style_are_removed(self):
        text = collector.notice_text(self.page("<p>公告编号：2020-017</p>"))
        self.assertIn("公告编号：2020-017", text)
        self.assertNotIn("color:red", text)
        self.assertNotIn("leaked", text)
        self.assertNotIn("<p>", text)


if __name__ == "__main__":
    unittest.main()
