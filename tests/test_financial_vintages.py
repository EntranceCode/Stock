"""The vintage check decides whether stored values are the published ones."""
import importlib.util
from pathlib import Path
import unittest

PROJECT = Path(__file__).resolve().parents[1]

try:
    SPEC = importlib.util.spec_from_file_location(
        "verify_financial_vintages", PROJECT / "scripts/verify_financial_vintages.py")
    checker = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(checker)
except ImportError as exc:
    checker = None
    SKIP_REASON = "optional dependency unavailable: " + str(exc)
else:
    SKIP_REASON = ""


def span(x0, y0, text, x1=None):
    return (float(x0), float(y0), float(x1 if x1 is not None else x0 + 40), text)


@unittest.skipUnless(checker is not None, SKIP_REASON)
class VintageCheckTests(unittest.TestCase):
    def test_label_row_takes_the_cells_printed_after_the_label(self):
        spans = [span(10, 100, "基本每股收益"), span(70, 100, "（元/股）"),
                 span(100, 100, "1.44"), span(130, 100, "1.45"),
                 span(10, 120, "稀释每股收益"), span(70, 120, "1.44")]
        self.assertEqual(checker.label_row(spans, checker.EPS_LABEL), ["（元/股）", "1.44", "1.45"])

    def test_deducted_variant_is_not_mistaken_for_the_plain_label(self):
        spans = [span(10, 100, "扣除非经常性损益后的基本每股收益"), span(200, 100, "1.00"),
                 span(10, 140, "基本每股收益"), span(80, 140, "9.99")]
        self.assertEqual(checker.label_row(spans, checker.EPS_LABEL), ["9.99"])

    def test_combined_basic_and_diluted_row_is_recognised(self):
        # Some issuers print both figures in one row: 基本/稀释每股收益（元/每股）.
        spans = [span(81, 443, "基本"), span(99, 443, "/"), span(102, 443, "稀释每股收益（元/每股）"),
                 span(355, 443, "0.33 "), span(452, 443, "0.31 ")]
        self.assertEqual(checker.label_row(spans, checker.EPS_LABEL), ["（元/每股）", "0.33 ", "0.31 "])

    def test_row_values_ignores_labels_units_and_percentage_cells(self):
        self.assertEqual(checker.row_values(["（元/股）", "1.44", "1.45", "-0.69%"]), [1.44, 1.45])
        self.assertEqual(checker.row_values([]), [])

    def test_compare_requires_the_stored_value_to_appear_among_printed_cells(self):
        self.assertEqual(checker.compare([1.44, 1.45], 1.44), "match")
        self.assertEqual(checker.compare([1.44, 1.45], 9.99), "mismatch")
        self.assertEqual(checker.compare([], 1.44), "not_extracted")
        self.assertEqual(checker.compare([1.44], None), "no_snapshot_row")


if __name__ == "__main__":
    unittest.main()
